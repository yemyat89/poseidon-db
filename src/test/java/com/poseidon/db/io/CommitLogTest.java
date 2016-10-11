package com.poseidon.db.io;

import static org.junit.Assert.*;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.IntStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.poseidon.db.TestUtils;
import com.poseidon.db.io.CommitLog;
import com.poseidon.db.representation.DataItem;
import com.poseidon.db.representation.KeyValuePair;
import com.poseidon.db.utils.Pair;

public class CommitLogTest {

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
	public void testWritingAndReloading() throws IOException {

		String commitLogPath = dataDir.getAbsolutePath() + File.separator + CommitLog.LOG_FILE_NAME_PREFIX + 111;
		CommitLog log = new CommitLog(commitLogPath, new ReentrantReadWriteLock());

		final HashSet<KeyValuePair> kvps1 = new HashSet<KeyValuePair>();
		final HashSet<KeyValuePair> kvps2 = new HashSet<KeyValuePair>();

		IntStream.range(0, 10000).forEach((i) -> {
			String keyStr = "test-key-" + i;
			String valueStr = "test-value-" + i;
			DataItem key = new DataItem(keyStr.getBytes());
			DataItem value = new DataItem(valueStr.getBytes());
			KeyValuePair kvp = new KeyValuePair(key, value);
			kvps1.add(kvp);

			try {
				log.writeToLog(CommitLog.LogOperation.PUT, kvp);
			} catch (Exception e) {
				e.printStackTrace();
			}
		});

		for (Pair<Byte, KeyValuePair> pair : log.getAllUnsavedOperations()) {
			assertEquals(pair.getLeft().byteValue(), CommitLog.LogOperation.PUT.getnumericValue());
			assertTrue(kvps1.contains(pair.getRight()));
			kvps2.add(pair.getRight());
		}

		assertEquals(kvps1.size(), kvps2.size());
		assertEquals(kvps1, kvps2);
	}

	@Test
	public void testDeleteRecord() throws IOException {
		String commitLogPath = dataDir.getAbsolutePath() + File.separator + CommitLog.LOG_FILE_NAME_PREFIX + 111;
		CommitLog log = new CommitLog(commitLogPath, new ReentrantReadWriteLock());

		final HashSet<KeyValuePair> kvps1 = new HashSet<KeyValuePair>();
		final HashSet<KeyValuePair> kvps2 = new HashSet<KeyValuePair>();

		IntStream.range(0, 10000).forEach((i) -> {
			String keyStr = "test-key-" + i;
			String valueStr = "test-value-" + i;
			DataItem key = new DataItem(keyStr.getBytes());
			DataItem value = new DataItem(valueStr.getBytes());
			KeyValuePair kvp = new KeyValuePair(key, value);
			kvps1.add(kvp);

			try {
				log.writeToLog(CommitLog.LogOperation.PUT, kvp);
			} catch (Exception e) {
				e.printStackTrace();
			}
		});

		IntStream.range(5000, 10000).forEach((i) -> {
			String keyStr = "test-key-" + i;
			String valueStr = "test-value-" + i;
			DataItem key = new DataItem(keyStr.getBytes());
			DataItem value = new DataItem(valueStr.getBytes());
			KeyValuePair kvp = new KeyValuePair(key, value);

			try {
				log.writeToLog(CommitLog.LogOperation.DELETE, kvp);
			} catch (Exception e) {
				e.printStackTrace();
			}
		});

		for (Pair<Byte, KeyValuePair> pair : log.getAllUnsavedOperations()) {
			assertTrue(kvps1.contains(pair.getRight()));

			if (pair.getLeft().byteValue() == CommitLog.LogOperation.DELETE.getnumericValue()) {
				kvps2.add(pair.getRight());
			}
		}

		assertEquals(kvps1.size()/2, kvps2.size());
	}

}
