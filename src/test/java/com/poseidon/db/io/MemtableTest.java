package com.poseidon.db.io;

import static org.junit.Assert.*;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.IntStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.poseidon.db.Memtable;
import com.poseidon.db.TestUtils;
import com.poseidon.db.io.CommitLog;
import com.poseidon.db.representation.DataItem;

public class MemtableTest {

	private File dataDir;

	@Before
	public void setUp() throws Exception {
		dataDir = new File("/tmp/geez");
		dataDir.mkdir();
	}

	@After
	public void tearDown() throws Exception {
		TestUtils.deleteFolder(dataDir);
	}

	@Test
	public void testPutAndGet() {
		Memtable mem = Memtable.createNewMemtable(new ReentrantReadWriteLock(), dataDir.getAbsolutePath());

		IntStream.range(0, 10000).forEach((i) -> {
			DataItem key = new DataItem(("test-key-" + i).getBytes());
			DataItem value = new DataItem(("test-value-" + i).getBytes());

			boolean putSuccess = mem.put(key, value);
			assertTrue(putSuccess);
		});

		assertTrue((((double) mem.numberOfItems()) / mem.size()) < Memtable.LOAD_FACTOR);

		IntStream.range(0, 10000).forEach((i) -> {
			DataItem key = new DataItem(("test-key-" + i).getBytes());
			DataItem expectedValue = new DataItem(("test-value-" + i).getBytes());

			DataItem valueReturned = mem.get(key);
			assertEquals(expectedValue, valueReturned);
		});
	}

	@Test
	public void testDelete() {
		Memtable mem = Memtable.createNewMemtable(new ReentrantReadWriteLock(), dataDir.getAbsolutePath());

		IntStream.range(0, 10000).forEach((i) -> {
			DataItem key = new DataItem(("test-key-" + i).getBytes());
			DataItem value = new DataItem(("test-value-" + i).getBytes());

			boolean putSuccess = mem.put(key, value);
			assertTrue(putSuccess);
		});

		IntStream.range(0, 10000).forEach((i) -> {
			DataItem key = new DataItem(("test-key-" + i).getBytes());
			
			boolean deleteSuccess = mem.delete(key, false);
			assertTrue(deleteSuccess);
		});

		IntStream.range(0, 10000).forEach((i) -> {
			DataItem key = new DataItem(("test-key-" + i).getBytes());

			DataItem value = mem.get(key);
			assertNull(value);
		});

		IntStream.range(0, 10000).forEach((i) -> {
			DataItem key = new DataItem(("test-key-" + i).getBytes());

			boolean deleteSuccess = mem.delete(key, true);
			assertTrue(deleteSuccess);
		});

		IntStream.range(0, 10000).forEach((i) -> {
			DataItem key = new DataItem(("test-key-" + i).getBytes());
			DataItem expectedValue = new DataItem(new byte[] { Memtable.TOMBSTONE });

			DataItem returnedValue = mem.get(key);
			assertEquals(expectedValue, returnedValue);
		});
	}

	@Test
	public void testReloadFromCommitLog() throws IOException {
		Memtable mem = Memtable.createNewMemtable(new ReentrantReadWriteLock(), dataDir.getAbsolutePath());

		IntStream.range(0, 10000).forEach((i) -> {
			DataItem key = new DataItem(("test-key-" + i).getBytes());
			DataItem value = new DataItem(("test-value-" + i).getBytes());

			boolean putSuccess = mem.put(key, value);
			assertTrue(putSuccess);
		});

		IntStream.range(5000, 10000).forEach((i) -> {
			DataItem key = new DataItem(("test-key-" + i).getBytes());
			boolean deleteSuccess = mem.delete(key, false);
			assertTrue(deleteSuccess);
		});

		IntStream.range(10000, 20000).forEach((i) -> {
			DataItem key = new DataItem(("test-key-" + i).getBytes());

			boolean deleteSuccess = mem.delete(key, true);
			assertTrue(deleteSuccess);
		});

		CommitLog commitLog = new CommitLog(mem.getLogFilePath(), new ReentrantReadWriteLock());
		Memtable mem2 = Memtable.createMemtableFromCommitLog(commitLog);

		IntStream.range(0, 5000).forEach((i) -> {
			DataItem key = new DataItem(("test-key-" + i).getBytes());
			DataItem expectedValue = new DataItem(("test-value-" + i).getBytes());

			DataItem valueReturned = mem2.get(key);
			assertEquals(expectedValue, valueReturned);
		});

		IntStream.range(5000, 10000).forEach((i) -> {
			DataItem key = new DataItem(("test-key-" + i).getBytes());
			DataItem valueReturned = mem2.get(key);
			assertEquals(null, valueReturned);
		});

		IntStream.range(10000, 20000).forEach((i) -> {
			DataItem key = new DataItem(("test-key-" + i).getBytes());
			DataItem expectedValue = new DataItem(new byte[] { Memtable.TOMBSTONE });

			DataItem valueReturned = mem2.get(key);
			assertEquals(expectedValue, valueReturned);
		});
	}

	@Test
	public void testReplaceValueInCommitLog() throws InterruptedException, IOException {
		Memtable mem = Memtable.createNewMemtable(new ReentrantReadWriteLock(), dataDir.getAbsolutePath());

		for (int i = 0; i < 1000; i++) {
			String key = "key-" + i;
			String value = "value-" + i + "*" + i;
			DataItem keyDt = new DataItem(key.getBytes());
			DataItem valueDt = new DataItem(value.getBytes());
			mem.put(keyDt, valueDt);
		}

		for (int i = 0; i < 1000; i++) {
			String key = "key-" + i;
			String value = "value-" + i + "+++" + i;
			DataItem keyDt = new DataItem(key.getBytes());
			DataItem valueDt = new DataItem(value.getBytes());
			mem.put(keyDt, valueDt);
		}

		Memtable mem2 = Memtable
				.createMemtableFromCommitLog(new CommitLog(mem.getLogFilePath(), new ReentrantReadWriteLock()));
		
		for (int i = 0; i < 1000; i++) {
			String key = "key-" + i;
			String value = "value-" + i + "+++" + i;
			DataItem keyDt = new DataItem(key.getBytes());
			DataItem valueDt = new DataItem(value.getBytes());
			assertEquals(mem2.get(keyDt), valueDt);
		}
	}
}
