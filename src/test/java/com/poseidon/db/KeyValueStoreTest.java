package com.poseidon.db;

import static org.junit.Assert.*;
import java.io.File;
import java.util.Arrays;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import com.poseidon.db.KeyValueStore;
import com.poseidon.db.io.access.FileAccessChoice;

public class KeyValueStoreTest {

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
	public void testSimplePutGetDelete() {

		for (FileAccessChoice fileAccess : new FileAccessChoice[] { FileAccessChoice.SIMPLE,
				FileAccessChoice.MEM_MAP }) {
			KeyValueStore store = KeyValueStore.getNewInstance(dataDir.getAbsolutePath(), fileAccess);

			byte[] key1 = "test-key-1".getBytes();
			byte[] value1 = "test-value-1".getBytes();
			byte[] key2 = "test-key-2".getBytes();
			byte[] value2 = "test-value-2".getBytes();

			store.put(key1, value1);
			store.put(key2, value2);

			assertTrue(Arrays.equals(store.get(key1), value1));
			assertTrue(Arrays.equals(store.get(key2), value2));

			store.delete(key2);

			assertTrue(Arrays.equals(store.get(key1), value1));
			assertTrue(Arrays.equals(store.get(key2), null));

			store.stop(false);
		}
	}

	@Test
	public void testOverridenValues() {
		for (FileAccessChoice fileAccess : new FileAccessChoice[] { FileAccessChoice.SIMPLE,
				FileAccessChoice.MEM_MAP }) {
			KeyValueStore store = KeyValueStore.getNewInstance(dataDir.getAbsolutePath(), fileAccess);

			byte[] key1 = "test-key-1".getBytes();
			byte[] value1 = "test-value-1".getBytes();

			store.put(key1, value1);
			assertTrue(Arrays.equals(store.get(key1), value1));

			byte[] value2 = "test-value-2".getBytes();
			store.put(key1, value2);

			assertTrue(Arrays.equals(store.get(key1), value2));
			store.stop(false);
		}
	}
	
	@Test
	public void testFlushingWithSimpleAccess() throws InterruptedException {
		testFlushingHelper(FileAccessChoice.SIMPLE);
	}

	@Test
	public void testFlushingWithMemMap() throws InterruptedException {
		testFlushingHelper(FileAccessChoice.MEM_MAP);
	}

	@Ignore
	@Test
	public void testHeavyWriteReadSimple() {
		KeyValueStore store = heavyWrite(dataDir.getAbsolutePath(), FileAccessChoice.SIMPLE);
		heavyRead(store);
		store.stop(false);
	}

	@Ignore
	@Test
	public void testHeavyWriteReadMemmap() {
		KeyValueStore store = heavyWrite(dataDir.getAbsolutePath(), FileAccessChoice.MEM_MAP);
		heavyRead(store);
		store.stop(false);
	}
	
	private void testFlushingHelper(FileAccessChoice fileAccess) throws InterruptedException {
		int N = 100;

		KeyValueStore store = KeyValueStore.getNewInstance(dataDir.getAbsolutePath(), fileAccess);

		for (int i = 0; i < N; i++) {
			String keyStr = "key-" + i;
			String valueStr = "value-" + i;

			store.put(keyStr.getBytes(), valueStr.getBytes());
			assertEquals(valueStr, new String(store.get(keyStr.getBytes())));
		}

		store.flush();

		for (int i = 0; i < N; i++) {
			String keyStr = "key-" + i;
			String valueStr = "value-" + i;
			assertEquals(valueStr, new String(store.get(keyStr.getBytes())));
		}

		for (int i = 0; i < N; i++) {
			String keyStr = "key-" + i;
			String valueStr = "value-" + i + "~" + i;

			store.put(keyStr.getBytes(), valueStr.getBytes());
			assertEquals(valueStr, new String(store.get(keyStr.getBytes())));
		}

		for (int i = 0; i < N; i++) {
			String keyStr = "key-" + i;
			String valueStr = "value-" + i + "~" + i;
			assertEquals(valueStr, new String(store.get(keyStr.getBytes())));
		}

		Thread.sleep(1000);
		store.flush();

		for (int i = 0; i < N; i++) {
			String keyStr = "key-" + i;
			String valueStr = "value-" + i + "~***~" + i;

			store.put(keyStr.getBytes(), valueStr.getBytes());
			assertEquals(valueStr, new String(store.get(keyStr.getBytes())));
		}

		Thread.sleep(1000);
		store.flush();

		for (int i = 0; i < N; i++) {
			String keyStr = "key-" + i;
			String valueStr = "value-" + i + "~***~" + i;
			assertEquals(valueStr, new String(store.get(keyStr.getBytes())));
		}

		// ------------------- with ExecutorService -----------------
		store.start();

		int M = N + 2000;
		for (int i = N; i < M; i++) {
			String keyStr = "key-" + i;
			String valueStr = "value-" + i + "==**==" + i;

			store.put(keyStr.getBytes(), valueStr.getBytes());
			assertEquals(valueStr, new String(store.get(keyStr.getBytes())));
		}

		Thread.sleep(10000);

		for (int i = N; i < M; i++) {
			String keyStr = "key-" + i;
			String valueStr = "value-" + i + "==**==" + i;
			assertEquals(valueStr, new String(store.get(keyStr.getBytes())));
		}

		for (int i = 0; i < (M + 120); i++) {
			String keyStr = "key-" + i;
			store.delete(keyStr.getBytes());
		}

		for (int i = 0; i < M; i++) {
			String keyStr = "key-" + i;
			assertEquals(store.get(keyStr.getBytes()), null);
		}

		store.stop(false);
	}
	
	private KeyValueStore heavyWrite(String dataDir, FileAccessChoice fileAccessChoice) {

		int N = 5000000, M = 500000;
		KeyValueStore store = KeyValueStore.getNewInstance(dataDir, fileAccessChoice);
		store.start();

		for (int i = 0; i < N; i++) {
			if (i % M == 0) {
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				System.err.println("Batch #" + (i / M) + " starts");
			}

			String keyStr = "key-" + i;
			String valueStr = "value-is-a-value~~~~value-is-a-value~~~~" + i;
			store.put(keyStr.getBytes(), valueStr.getBytes());
		}

		System.out.println("Done Writing.");
		return store;
	}

	private void heavyRead(KeyValueStore store) {
		int N = 5000000, M = 500000;

		for (int i = 0; i < N; i++) {
			String keyStr = "key-" + i;
			String valueStr = "value-is-a-value~~~~value-is-a-value~~~~" + i;

			if (i % M == 0) {
				System.out.println("Done - " + (i / M));
			}

			byte[] result = store.get(keyStr.getBytes());

			if (result != null) {
				String valueStrReturned = new String(result);
				if (!valueStrReturned.equals(valueStr)) {
					System.err.println("Expected result not equal for - " + keyStr + " => " + valueStrReturned);
				}
			} else {
				System.err.println("Expected result not found for - " + keyStr);
				break;
			}
		}

		System.out.println("Done Reading.");
	}

}
