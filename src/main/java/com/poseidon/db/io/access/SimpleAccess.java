package com.poseidon.db.io.access;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import com.poseidon.db.representation.DataItem;
import com.poseidon.db.utils.DataConversion;
import com.poseidon.db.utils.Pair;

public class SimpleAccess implements FileAccess {

	private RandomAccessFile sstableFile;

	public SimpleAccess(RandomAccessFile sstableFile) {
		this.sstableFile = sstableFile;
	}

	@Override
	public DataItem getValue(DataItem key, long startOffset, long limitOffset) throws IOException {
		sstableFile.seek(startOffset);

		while (true) {
			byte[] bufRecordLength = new byte[4];
			byte[] bufKeyLength = new byte[4];

			sstableFile.read(bufRecordLength);
			sstableFile.read(bufKeyLength);

			byte[] keyData = new byte[DataConversion.byteArrayToInt(bufKeyLength)];
			byte[] valueData = new byte[DataConversion.byteArrayToInt(bufRecordLength)
					- DataConversion.byteArrayToInt(bufKeyLength)];

			sstableFile.read(keyData);
			sstableFile.read(valueData);

			DataItem keyDt = new DataItem(keyData);
			DataItem valueDt = new DataItem(valueData);

			if (keyDt.equals(key)) {
				return valueDt;
			}

			if (keyDt.compareTo(key) > 0 || sstableFile.getFilePointer() == limitOffset) {
				break;
			}
		}

		return null;
	}

	@Override
	public Pair<DataItem, List<Pair<DataItem, DataItem>>> getValueAndLoadBlock(DataItem key, long startOffset,
			long limitOffset) throws IOException {

		DataItem found = null;
		List<Pair<DataItem, DataItem>> block = new ArrayList<Pair<DataItem, DataItem>>();

		sstableFile.seek(startOffset);

		while (true) {
			byte[] bufRecordLength = new byte[4];
			byte[] bufKeyLength = new byte[4];

			sstableFile.read(bufRecordLength);
			sstableFile.read(bufKeyLength);

			byte[] keyData = new byte[DataConversion.byteArrayToInt(bufKeyLength)];
			byte[] valueData = new byte[DataConversion.byteArrayToInt(bufRecordLength)
					- DataConversion.byteArrayToInt(bufKeyLength)];

			sstableFile.read(keyData);
			sstableFile.read(valueData);

			DataItem keyDt = new DataItem(keyData);
			DataItem valueDt = new DataItem(valueData);

			block.add(new Pair<DataItem, DataItem>(keyDt, valueDt));

			if (keyDt.equals(key)) {
				found = valueDt;
			}

			if (sstableFile.getFilePointer() >= limitOffset) {
				break;
			}
		}

		return new Pair<DataItem, List<Pair<DataItem, DataItem>>>(found, block);
	}
}
