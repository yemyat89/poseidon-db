package com.poseidon.db;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.log4j.Logger;
import com.poseidon.db.hash.Hash;
import com.poseidon.db.hash.SimpleModuloHash;
import com.poseidon.db.io.CommitLog;
import com.poseidon.db.io.CommitLog.LogOperation;
import com.poseidon.db.representation.DataItem;
import com.poseidon.db.representation.KeyValuePair;
import com.poseidon.db.utils.Pair;

public class Memtable {

	private final static Logger logger = Logger.getLogger(Memtable.class);
	public static final int INITIAL_CAPACITY = 16;
	public static final byte TOMBSTONE = 0;
	public static final double LOAD_FACTOR = 0.75;

	private int capacity;
	private int itemCount;
	private String identity;
	private KeyValuePair[] kvPairs;
	private Hash hashFunction;
	private CommitLog commitLog;
	private ReentrantReadWriteLock rwLock;
	private int totalByteCount;

	public static Memtable createNewMemtable(ReentrantReadWriteLock rwLock, String logFilePath) {
		return new Memtable(rwLock, logFilePath);
	}

	public static Memtable createMemtableFromCommitLog(CommitLog commitLog) {
		Memtable mem = new Memtable(commitLog.getLock(), commitLog);

		for (Pair<Byte, KeyValuePair> pair : commitLog.getAllUnsavedOperations()) {
			if (pair.getLeft().equals(LogOperation.PUT.getnumericValue())) {
				mem.put(pair.getRight().getKey(), pair.getRight().getValue(), false);
			} else if (pair.getLeft().equals(LogOperation.DELETE.getnumericValue())) {
				mem.delete(pair.getRight().getKey(), false);
			}
		}

		return mem;
	}

	public Hash getHashFunction() {
		return hashFunction;
	}

	public DataItem get(DataItem key) {
		rwLock.readLock().lock();

		try {
			int index = calculateIndex(key);

			KeyValuePair kvp = kvPairs[index];
			while (kvp != null) {
				if (kvp.getKey().equals(key)) {
					return kvp.getValue();
				}
				kvp = kvp.getNext();
			}

		} finally {
			rwLock.readLock().unlock();
		}

		return null;
	}

	public boolean put(DataItem key, DataItem value) {
		return put(key, value, true);
	}

	public boolean put(DataItem key, DataItem value, boolean shouldLog) {
		rwLock.writeLock().lock();

		try {
			int index = calculateIndex(key);
			KeyValuePair kvpFound = kvPairs[index];

			while (kvpFound != null) {
				if (kvpFound.getKey().equals(key)) {
					
					totalByteCount -= kvpFound.getValue().getData().length;
					totalByteCount += value.getData().length;
					
					kvpFound.setValue(value);

					if (shouldLog) {
						commitLog.writeToLog(CommitLog.LogOperation.PUT, kvpFound);
					}

					return true;
				}
				kvpFound = kvpFound.getNext();
			}

			KeyValuePair kvpCurrent = new KeyValuePair(key, value);
			kvpCurrent.setNext(kvPairs[index]);
			kvPairs[index] = kvpCurrent;
			
			totalByteCount += kvpCurrent.getKey().getData().length;
			totalByteCount += kvpCurrent.getValue().getData().length;

			if (shouldLog) {
				commitLog.writeToLog(CommitLog.LogOperation.PUT, kvpCurrent);
			}

			if (needsResize()) {
				resize();
			}

			itemCount++;

		} catch (IOException e) {
			logger.error("Failed to write to commit log file - " + commitLog.getLogFilePath());
			return false;
		} finally {
			rwLock.writeLock().unlock();
		}
		return true;
	}

	public boolean delete(DataItem key, boolean markIfNotFound) {
		rwLock.writeLock().lock();

		try {
			DataItem valueDt = get(key);

			if (valueDt != null) {
				
				totalByteCount -= key.getData().length;
				totalByteCount -= valueDt.getData().length;
				
				put(key, null, false);
				commitLog.writeToLog(LogOperation.DELETE, new KeyValuePair(key, valueDt));
			} else if (markIfNotFound) {
				put(key, new DataItem(new byte[] { TOMBSTONE }));
			}

			return true;
		} catch (IOException e) {
			logger.error("Failed to write to commit log file - " + commitLog.getLogFilePath());
		} finally {
			rwLock.writeLock().unlock();
		}
		return false;
	}

	public List<KeyValuePair> getAllKeyValuePairs() {
		List<KeyValuePair> results = new ArrayList<KeyValuePair>();

		for (KeyValuePair kvp : kvPairs) {
			if (kvp == null) {
				continue;
			}
			while (kvp != null) {
				results.add(kvp);
				kvp = kvp.getNext();
			}
		}

		return results;
	}

	public boolean empty() {
		rwLock.writeLock().lock();
		try {
			return (itemCount == 0);
		} finally {
			rwLock.writeLock().unlock();
		}
	}

	public void destroyCommitLog() {
		commitLog.destroy();
	}

	public int size() {
		return capacity;
	}

	public int numberOfItems() {
		return itemCount;
	}
	
	public int getTotalByteCount() {
		return totalByteCount;
	}

	public String getLogFilePath() {
		return commitLog.getLogFilePath();
	}

	public void cleanUp() {
		commitLog.closeLogFile();
	}

	private Memtable(ReentrantReadWriteLock rwLock) {
		identity = UUID.randomUUID().toString();
		kvPairs = new KeyValuePair[INITIAL_CAPACITY];
		hashFunction = new SimpleModuloHash(INITIAL_CAPACITY);
		itemCount = 0;
		totalByteCount = 0;
		capacity = INITIAL_CAPACITY;

		this.rwLock = rwLock;
	}

	private Memtable(ReentrantReadWriteLock rwLock, CommitLog commitLog) {
		this(rwLock);
		this.commitLog = commitLog;
	}

	private Memtable(ReentrantReadWriteLock rwLock, String logFilePath) {
		this(rwLock);

		String commitLogPath = logFilePath + File.separator + CommitLog.LOG_FILE_NAME_PREFIX + identity;
		try {
			commitLog = new CommitLog(commitLogPath, rwLock);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private boolean needsResize() {
		double loadFactor = ((double) itemCount) / capacity;
		return (loadFactor >= LOAD_FACTOR);
	}

	private void resize() {
		capacity = (capacity << 1);
		KeyValuePair[] tmpKvPairs = new KeyValuePair[capacity];
		hashFunction.setCapacity(capacity);

		for (KeyValuePair kvp : kvPairs) {
			KeyValuePair currentKvp = kvp;
			while (currentKvp != null) {
				int newIndex = calculateIndex(currentKvp.getKey());
				KeyValuePair nextKvp = currentKvp.getNext();
				currentKvp.setNext(tmpKvPairs[newIndex]);
				tmpKvPairs[newIndex] = currentKvp;
				currentKvp = nextKvp;
			}
		}
		kvPairs = tmpKvPairs;
	}

	private int calculateIndex(DataItem key) {
		return hashFunction.calculateHash(key);
	}
}
