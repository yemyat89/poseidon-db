package com.poseidon.db.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.log4j.Logger;
import com.poseidon.db.Memtable;
import com.poseidon.db.io.access.FileAccess;
import com.poseidon.db.io.access.FileAccessChoice;
import com.poseidon.db.io.access.SimpleAccess;
import com.poseidon.db.io.access.SimpleMappedByteBufferAccess;
import com.poseidon.db.representation.DataItem;
import com.poseidon.db.representation.KeyValuePair;
import com.poseidon.db.utils.BlockCache;
import com.poseidon.db.utils.BloomFilter;
import com.poseidon.db.utils.DataConversion;
import com.poseidon.db.utils.Pair;

public class SSTable {

	private final static Logger logger = Logger.getLogger(CommitLog.class);
	public static final String SSTABLE_FILENAME_PREFIX = "sstable-";
	public static final int INDEX_INTERVAL = 128;

	public static TreeMap<String, SSTable> sstables = new TreeMap<String, SSTable>();

	static class IndexData {
		private DataItem key;
		private long offset;
		private int byteCount;

		public int getByteCount() {
			return byteCount;
		}

		public void setByteCount(int byteCount) {
			this.byteCount = byteCount;
		}

		IndexData(DataItem key, long offset) {
			this(key, offset, 0);
		}

		IndexData(DataItem key, long offset, int byteCount) {
			setKey(key);
			setOffset(offset);
			setByteCount(byteCount);
		}

		public DataItem getKey() {
			return key;
		}

		public void setKey(DataItem key) {
			this.key = key;
		}

		public long getOffset() {
			return offset;
		}

		public void setOffset(long offset) {
			this.offset = offset;
		}
	}

	private int itemCount;
	private long indexPosition;
	private long filterPosition;
	private String sstableFilePath;
	private BloomFilter bloomFilter;
	private RandomAccessFile sstableFile;
	private FileAccess fileAccess;
	private IndexData[] indexes;
	private BlockCache<DataItem, List<Pair<DataItem, DataItem>>> blockCache;

	public static DataItem find(DataItem key) {

		List<SSTable> sstables = new ArrayList<SSTable>();

		synchronized (SSTable.class) {
			for (String sstableFileName : SSTable.sstables.descendingKeySet()) {
				sstables.add(SSTable.sstables.get(sstableFileName));
			}
		}

		for (SSTable sstable : sstables) {
			try {
				DataItem valueDt = sstable.get(key);
				if (valueDt != null) {
					return valueDt;
				}
			} catch (IOException e) {
				logger.error("Cannot find inside sstable file - " + sstable.getSSTableFilePath());
			}
		}

		return null;
	}

	public void closeSSTableFile() {
		try {
			sstableFile.close();
		} catch (IOException e) {
			logger.warn("Cannot close sstable file - " + getSSTableFilePath());
		}
	}

	public static void createAndRegisterNewSSTable(String sstableFilePath, FileAccessChoice fileAccessChoice)
			throws IOException {
		synchronized (SSTable.class) {
			SSTable.sstables.put(sstableFilePath, new SSTable(sstableFilePath, fileAccessChoice));
		}
	}

	public static void createAndRegisterNewSSTable(Memtable mem, String sstablePath, FileAccessChoice fileAccessChoice)
			throws IOException {

		BloomFilter bloomFilter = new BloomFilter();
		int itemCount = 0;
		String identity = UUID.randomUUID().toString();
		long unixTime = System.currentTimeMillis() / 1000L;
		String sstableFilePath = sstablePath + File.separator + SSTABLE_FILENAME_PREFIX + unixTime + "-" + identity;
		List<IndexData> indexList = new ArrayList<IndexData>();
		RandomAccessFile sstableFile = new RandomAccessFile(sstableFilePath, "rw");

		sstableFile.seek(24);

		List<KeyValuePair> kvps = mem.getAllKeyValuePairs();
		Collections.sort(kvps);
		int blockByteCount = 0;
		int totalByteCount = 0;

		for (KeyValuePair kvp : kvps) {
			int keyLength = kvp.getKey().length();
			int recordLength = keyLength + kvp.getValue().length();

			bloomFilter.add(kvp.getKey().getData());

			if ((itemCount % INDEX_INTERVAL) == 0) {
				if (indexList.size() > 0) {
					indexList.get(indexList.size() - 1).setByteCount(blockByteCount);
				}
				blockByteCount = 0;
				indexList.add(new IndexData(kvp.getKey(), sstableFile.getFilePointer()));
			}

			sstableFile.write(DataConversion.intToByteArray(recordLength));
			sstableFile.write(DataConversion.intToByteArray(keyLength));
			sstableFile.write(kvp.getKey().getData());
			sstableFile.write(kvp.getValue().getData());

			blockByteCount += 4 + 4 + recordLength;
			totalByteCount += 4 + 4 + recordLength;
			;
			itemCount++;
		}
		indexList.get(indexList.size() - 1).setByteCount(blockByteCount);

		long indexPosition = sstableFile.getFilePointer();
		for (IndexData idPair : indexList) {
			sstableFile.write(DataConversion.intToByteArray(idPair.getKey().length()));
			sstableFile.write(idPair.getKey().getData());
			sstableFile.write(DataConversion.intToByteArray(idPair.getByteCount()));
			sstableFile.write(DataConversion.longToByteArray(idPair.getOffset()));
		}

		long filterPosition = sstableFile.getFilePointer();
		byte[] filterData = bloomFilter.getByteArray();
		sstableFile.write(filterData);

		sstableFile.seek(0);

		sstableFile.write(DataConversion.intToByteArray(itemCount));
		sstableFile.write(DataConversion.intToByteArray(totalByteCount));
		sstableFile.write(DataConversion.longToByteArray(indexPosition));
		sstableFile.write(DataConversion.longToByteArray(filterPosition));

		IndexData[] indexes = indexList.toArray(new IndexData[indexList.size()]);
		SSTable sstable = new SSTable(sstableFile, indexes, indexPosition, filterPosition, itemCount, bloomFilter,
				sstableFilePath, fileAccessChoice);

		synchronized (SSTable.class) {
			sstables.put(sstableFilePath, sstable);
		}
	}

	public String getSSTableFilePath() {
		return sstableFilePath;
	}

	public int numberOfItems() {
		return itemCount;
	}

	public IndexData[] getIndexes() {
		return indexes;
	}

	private SSTable(RandomAccessFile sstableFile, IndexData[] indexes, long indexPosition, long filterPosition,
			int itemCount, BloomFilter bloomFilter, String sstableFilePath, FileAccessChoice fileAccessChoice)
					throws IOException {
		this.sstableFile = sstableFile;
		this.indexes = indexes;
		this.itemCount = itemCount;
		this.indexPosition = indexPosition;
		this.filterPosition = filterPosition;
		this.bloomFilter = bloomFilter;
		this.sstableFilePath = sstableFilePath;
		blockCache = new BlockCache<DataItem, List<Pair<DataItem, DataItem>>>(100);

		initializeFileAccess(fileAccessChoice);
	}

	private SSTable(String sstableFilePath, FileAccessChoice fileAccessChoice) throws IOException {
		this.sstableFilePath = sstableFilePath;
		sstableFile = new RandomAccessFile(sstableFilePath, "rw");
		blockCache = new BlockCache<DataItem, List<Pair<DataItem, DataItem>>>(100);
		readSSTableFile();
		initializeFileAccess(fileAccessChoice);
	}

	private final void readSSTableFile() throws IOException {
		byte[] buf = new byte[4];

		sstableFile.read(buf);
		itemCount = DataConversion.byteArrayToInt(buf);

		sstableFile.read(buf);
		DataConversion.byteArrayToInt(buf); // TODO: to use total byte count

		buf = new byte[8];
		sstableFile.read(buf);
		indexPosition = DataConversion.byteArrayToLong(buf);

		buf = new byte[8];
		sstableFile.read(buf);
		filterPosition = DataConversion.byteArrayToLong(buf);

		loadIndexes(indexPosition, itemCount);
	}

	private final void loadIndexes(long indexPosition, int itemCount) throws IOException {
		sstableFile.seek(indexPosition);

		byte[] keyLengthBuf = new byte[4];
		byte[] byteCountBuf = new byte[4];
		byte[] offsetBuf = new byte[8];

		List<IndexData> indexList = new ArrayList<IndexData>();

		while (sstableFile.getFilePointer() < filterPosition) {
			sstableFile.read(keyLengthBuf);

			byte[] keyBuf = new byte[DataConversion.byteArrayToInt(keyLengthBuf)];

			sstableFile.read(keyBuf);
			sstableFile.read(byteCountBuf);
			sstableFile.read(offsetBuf);

			indexList.add(new IndexData(new DataItem(keyBuf), DataConversion.byteArrayToLong(offsetBuf),
					DataConversion.byteArrayToInt(byteCountBuf)));
		}

		indexes = indexList.toArray(new IndexData[indexList.size()]);

		byte[] filterBuf = new byte[(int) (sstableFile.length() - filterPosition)];
		sstableFile.read(filterBuf);
		bloomFilter = new BloomFilter(filterBuf);
	}

	private IndexData binarySearch(DataItem key) {
		int start = 0;
		int end = indexes.length - 1;
		int found = -1;

		while (start <= end) {
			int mid = start + ((end - start) / 2);
			IndexData midIndexPair = indexes[mid];

			if (midIndexPair.getKey().compareTo(key) == 0) {
				found = mid;
				break;
			} else if (midIndexPair.getKey().compareTo(key) > 0) {
				end = mid - 1;
			} else {
				start = mid + 1;
			}
		}

		if (found < 0) {
			found = end;
		}

		if (found >= 0) {
			return indexes[found];
		}

		return null;
	}

	private DataItem get(DataItem key) throws IOException {
		if (!bloomFilter.contains(key.getData())) {
			return null;
		}

		IndexData indexPair = binarySearch(key);

		if (indexPair != null) {

			if (blockCache.containsKey(indexPair.getKey())) {
				for (Pair<DataItem, DataItem> pair : blockCache.get(indexPair.getKey())) {
					if (pair.getLeft().equals(key)) {
						return pair.getRight();
					}
				}
				return null;
			} else {
				Pair<DataItem, List<Pair<DataItem, DataItem>>> pair = fileAccess.getValueAndLoadBlock(key,
						indexPair.getOffset(), indexPair.getOffset() + indexPair.getByteCount());

				if (!pair.getRight().isEmpty()) {
					blockCache.put(indexPair.getKey(), pair.getRight());
				}
				return pair.getLeft();
			}
		}

		return null;
	}

	private void initializeFileAccess(FileAccessChoice fileAccessChoice) throws IOException {
		if (fileAccessChoice == FileAccessChoice.SIMPLE) {
			fileAccess = new SimpleAccess(sstableFile);
		} else if (fileAccessChoice == FileAccessChoice.MEM_MAP) {
			fileAccess = new SimpleMappedByteBufferAccess(sstableFile);
		}
	}
}
