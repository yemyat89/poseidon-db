package com.poseidon.db;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.log4j.Logger;
import com.poseidon.db.io.CommitLog;
import com.poseidon.db.io.SSTable;
import com.poseidon.db.io.access.FileAccessChoice;
import com.poseidon.db.representation.DataItem;

public class KeyValueStore {

	private final static Logger logger = Logger.getLogger(KeyValueStore.class);
	public static final int MEM_TO_SSTABLE_THRESHOLD = 128 * 1024 * 1024;

	private static class MemtableSelector {
		private Memtable primaryMemtable;
		private Memtable secondaryMemtable;
		private ReentrantReadWriteLock rwLock;
		private String dataDir;

		public MemtableSelector(ReentrantReadWriteLock rwLock, String dataDir) {
			primaryMemtable = Memtable.createNewMemtable(rwLock, dataDir);
			secondaryMemtable = Memtable.createNewMemtable(rwLock, dataDir);
			this.rwLock = rwLock;
			this.dataDir = dataDir;
		}

		public MemtableSelector(ReentrantReadWriteLock rwLock, String dataDir, Memtable primaryMemtable,
				Memtable secondaryMemtable) {
			this.primaryMemtable = primaryMemtable;
			this.secondaryMemtable = secondaryMemtable;
			this.rwLock = rwLock;
			this.dataDir = dataDir;
		}

		public Memtable getPrimaryMemtable() {
			return primaryMemtable;
		}

		public Memtable getSecondaryMemtable() {
			return secondaryMemtable;
		}

		public void switchPrimaryMemtable() {
			rwLock.writeLock().lock();
			try {
				if (!secondaryMemtable.empty()) {
					logger.warn("Secondary memtable is expected to be empty but not");
				} else {
					Memtable tempMemtable = secondaryMemtable;
					secondaryMemtable = primaryMemtable;
					primaryMemtable = tempMemtable;
				}
			} finally {
				rwLock.writeLock().unlock();
			}
		}

		public void resetSecondaryMemtable() {
			rwLock.writeLock().lock();
			try {
				secondaryMemtable.destroyCommitLog();
				secondaryMemtable = Memtable.createNewMemtable(rwLock, dataDir);
			} finally {
				rwLock.writeLock().unlock();
			}
		}

		public ReentrantReadWriteLock getLock() {
			return rwLock;
		}
	}

	private MemtableSelector memtableSelector;
	private ReentrantReadWriteLock rwLock;
	private String dataDir;
	private ScheduledExecutorService executorService;
	private FileAccessChoice fileAccessChoice;

	public static KeyValueStore getNewInstance(String dataDir, FileAccessChoice fileAccessChoice) {
		File dataFilesRoot = new File(dataDir);

		File[] commitLogFiles = dataFilesRoot.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.startsWith("commit-log-");
			}
		});

		File[] sstableFiles = dataFilesRoot.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.startsWith("sstable-");
			}
		});

		if (commitLogFiles.length > 0) {
			return reloadExistingData(dataDir, commitLogFiles, sstableFiles, fileAccessChoice);
		}
		return new KeyValueStore(dataDir, fileAccessChoice);
	}

	public void start() {
		executorService.scheduleWithFixedDelay(() -> {
			flushToSSTable();
		} , 5, 5, TimeUnit.SECONDS);
	}

	public void stop(boolean force) {

		memtableSelector.getPrimaryMemtable().cleanUp();
		memtableSelector.getSecondaryMemtable().cleanUp();

		if (force) {
			executorService.shutdownNow();
		} else {
			executorService.shutdown();
		}
	}

	public byte[] get(byte[] key) {
		DataItem foundValueDt = memtableSelector.getPrimaryMemtable().get(new DataItem(key));

		if (foundValueDt == null) {
			return getFromSecondaryData(key);
		} else {
			if (foundValueDt.equals(new DataItem(new byte[] { Memtable.TOMBSTONE }))) {
				return null;
			} else {
				return foundValueDt.getData();
			}
		}
	}

	public boolean put(byte[] key, byte[] value) {
		Memtable mem = memtableSelector.getPrimaryMemtable();
		return mem.put(new DataItem(key), new DataItem(value));
	}

	public boolean delete(byte[] key) {
		Memtable mem = memtableSelector.getPrimaryMemtable();
		return mem.delete(new DataItem(key), (getFromSecondaryData(key) != null));
	}

	public void flush() {
		memtableSelector.switchPrimaryMemtable();
		try {
			SSTable.createAndRegisterNewSSTable(memtableSelector.getSecondaryMemtable(), dataDir, fileAccessChoice);
		} catch (IOException e) {
			throw new RuntimeException("Failed to flush in-memory data to disk.", e);
		}
		memtableSelector.resetSecondaryMemtable();
	}

	private KeyValueStore(String dataDir, FileAccessChoice fileAccessChoice) {
		this.rwLock = new ReentrantReadWriteLock();
		this.dataDir = dataDir;
		this.fileAccessChoice = fileAccessChoice;

		executorService = Executors.newScheduledThreadPool(1);
		memtableSelector = new MemtableSelector(rwLock, this.dataDir);
	}

	private KeyValueStore(String dataDir, MemtableSelector memtableSelector, FileAccessChoice fileAccessChoice) {
		this.dataDir = dataDir;
		this.memtableSelector = memtableSelector;
		this.rwLock = this.memtableSelector.getLock();
		this.fileAccessChoice = fileAccessChoice;

		executorService = Executors.newScheduledThreadPool(1);

	}

	private static KeyValueStore reloadExistingData(String dataDir, File[] commitLogFiles, File[] sstableFiles,
			FileAccessChoice fileAccessChoice) {
		ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
		Memtable[] memtables = createPrimaryMemtable(commitLogFiles, rwLock);

		if (memtables.length < 2) {
			throw new RuntimeException(
					"Needs at least 2 commit logs to reload the existing data. Found only " + memtables.length);
		}

		for (File s : sstableFiles) {
			try {
				SSTable.createAndRegisterNewSSTable(s.getAbsolutePath(), fileAccessChoice);
			} catch (IOException e) {
				logger.error("Cannot load existing sstable file - " + s.getAbsolutePath());
			}
		}

		MemtableSelector memtableSelector = new MemtableSelector(rwLock, dataDir, memtables[memtables.length - 1],
				memtables[memtables.length - 2]);
		return new KeyValueStore(dataDir, memtableSelector, fileAccessChoice);
	}

	private static Memtable[] createPrimaryMemtable(File[] commitLogFiles, ReentrantReadWriteLock rwLock) {

		List<CommitLog> logsList = new ArrayList<CommitLog>();
		for (int i = 0; i < commitLogFiles.length; i++) {
			try {
				logsList.add(new CommitLog(commitLogFiles[i].getAbsolutePath(), rwLock));
			} catch (IOException e) {
				logger.error("Failed to load commit log file - " + commitLogFiles[i].getAbsolutePath());
			}
		}

		CommitLog[] logs = logsList.toArray(new CommitLog[logsList.size()]);
		Memtable[] memtables = new Memtable[logs.length];
		Arrays.sort(logs, (a, b) -> ((int) (a.getLastModifiedTime() - b.getLastModifiedTime())));

		for (int i = logs.length - 1; i >= 0; i--) {
			memtables[i] = Memtable.createMemtableFromCommitLog(logs[i]);
		}

		return memtables;
	}

	private final void flushToSSTable() {
		if (memtableSelector.getPrimaryMemtable().getTotalByteCount() >= MEM_TO_SSTABLE_THRESHOLD) {
			logger.info("Flushing " + memtableSelector.getPrimaryMemtable().numberOfItems() + " items to sstable.");
			flush();
		}
	}

	private final byte[] getFromSecondaryData(byte[] key) {
		DataItem keyDt = new DataItem(key);
		DataItem foundValueDt = memtableSelector.getSecondaryMemtable().get(keyDt);

		if (foundValueDt == null) {
			foundValueDt = SSTable.find(keyDt);
		}

		if (foundValueDt == null || foundValueDt.equals(new DataItem(new byte[] { Memtable.TOMBSTONE }))) {
			return null;
		}
		return foundValueDt.getData();
	}
}
