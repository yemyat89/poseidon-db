package com.poseidon.db.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.log4j.Logger;
import com.poseidon.db.representation.DataItem;
import com.poseidon.db.representation.KeyValuePair;
import com.poseidon.db.utils.DataConversion;
import com.poseidon.db.utils.Pair;

public class CommitLog {

	private final static Logger logger = Logger.getLogger(CommitLog.class);
	public static final String LOG_FILE_NAME_PREFIX = "commit-log-";

	public static enum LogOperation {
		PUT(0), GET(1), DELETE(2);

		private byte numericValue;

		LogOperation(int numericValue) {
			this.numericValue = (byte) (numericValue & 0xff);
		}

		public byte getnumericValue() {
			return numericValue;
		}
	}

	private File file;
	private RandomAccessFile logFile;
	private ReentrantReadWriteLock rwLock;

	public CommitLog(String logFilePath, ReentrantReadWriteLock rwLock) throws IOException {
		file = new File(logFilePath);
		this.logFile = new RandomAccessFile(file, "rw");
		this.rwLock = rwLock;
		logFile.seek(logFile.length());
	}

	public long getLastModifiedTime() {
		BasicFileAttributes attributes;
		try {
			attributes = Files.readAttributes(Paths.get(file.getAbsolutePath()), BasicFileAttributes.class);
			return attributes.lastModifiedTime().toMillis();
		} catch (IOException e) {
			// Cannot find last modified time for some reason, falls back to
			// default (0L)
		}
		return 0L;
	}

	public void destroy() {
		try {
			this.logFile.close();
		} catch (IOException e) {
			// File cannot be close, best effort.
		}
		file.delete();
	}

	public void writeToLog(LogOperation op, KeyValuePair kvp) throws IOException {
		logFile.write(op.getnumericValue());
		kvp.writeToFile(logFile);
	}

	public String getLogFilePath() {
		return file.getAbsolutePath();
	}

	public List<Pair<Byte, KeyValuePair>> getAllUnsavedOperations() {

		List<Pair<Byte, KeyValuePair>> results = new ArrayList<Pair<Byte, KeyValuePair>>();

		rwLock.writeLock().lock();
		try {
			logFile.seek(0);

			while (logFile.getFilePointer() < logFile.length()) {
				byte[] bufOp = new byte[1];
				byte[] bufRecordLength = new byte[4];
				byte[] bufKeyLength = new byte[4];

				logFile.read(bufOp);
				logFile.read(bufRecordLength);
				logFile.read(bufKeyLength);

				if (LogOperation.PUT.getnumericValue() == bufOp[0]
						|| LogOperation.DELETE.getnumericValue() == bufOp[0]) {
					byte[] keyData = new byte[DataConversion.byteArrayToInt(bufKeyLength)];
					byte[] valueData = new byte[DataConversion.byteArrayToInt(bufRecordLength)
							- DataConversion.byteArrayToInt(bufKeyLength)];

					logFile.read(keyData);
					logFile.read(valueData);

					Pair<Byte, KeyValuePair> result = new Pair<Byte, KeyValuePair>(bufOp[0],
							new KeyValuePair(new DataItem(keyData), new DataItem(valueData)));
					results.add(result);
				}
			}
		} catch (IOException e) {
			// Cannot seek, just return with no results
		} finally {
			rwLock.writeLock().unlock();
		}

		return results;
	}

	public ReentrantReadWriteLock getLock() {
		return rwLock;
	}

	public void closeLogFile() {
		try {
			logFile.close();
		} catch (IOException e) {
			logger.warn("Unable to close commit log file - " + getLogFilePath());
		}
	}
}
