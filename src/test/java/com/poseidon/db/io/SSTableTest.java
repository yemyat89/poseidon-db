package com.poseidon.db.io;

import static org.junit.Assert.*;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.poseidon.db.Memtable;
import com.poseidon.db.TestUtils;
import com.poseidon.db.io.SSTable;
import com.poseidon.db.io.SSTable.IndexData;
import com.poseidon.db.io.access.FileAccessChoice;
import com.poseidon.db.representation.DataItem;

public class SSTableTest {

	private File dataDir;

	@Before
	public void setUp() throws Exception {
		dataDir = new File("/tmp/geez");
		dataDir.mkdir();
	}

	@After
	public void tearDown() throws Exception {
		TestUtils.deleteFolder(dataDir);
		SSTable.sstables.clear();
	}

	@Test
	public void testFindFromSSTable() throws IOException, InterruptedException {
		Memtable mem = Memtable.createNewMemtable(new ReentrantReadWriteLock(), dataDir.getAbsolutePath());

		for (FileAccessChoice fileAccess : new FileAccessChoice[] { FileAccessChoice.SIMPLE,
				FileAccessChoice.MEM_MAP }) {

			for (int i = 0; i < 1000; i++) {
				String keyStr = "key-" + String.format("%03d", i);
				String valueStr = "value-" + String.format("%03d", i);

				mem.put(new DataItem(keyStr.getBytes()), new DataItem(valueStr.getBytes()));
			}

			SSTable.createAndRegisterNewSSTable(mem, dataDir.getAbsolutePath(), fileAccess);

			for (int i = 0; i < 1000; i++) {
				String keyStr = "key-" + String.format("%03d", i);
				String valueStr = "value-" + String.format("%03d", i);

				DataItem v = SSTable.find(new DataItem(keyStr.getBytes()));

				assertEquals(valueStr, new String(v.getData()));
			}

			// Thread.sleep(1000);
		}
	}

	@Test
	public void testFindFromSSTableAfterReplace() throws IOException, InterruptedException {

		for (FileAccessChoice fileAccess : new FileAccessChoice[] { FileAccessChoice.SIMPLE }) {
			Memtable mem1 = Memtable.createNewMemtable(new ReentrantReadWriteLock(), dataDir.getAbsolutePath());
			for (int i = 0; i < 1000; i++) {
				String keyStr = "key-" + String.format("%03d", i);
				String valueStr = "value-" + String.format("%03d", i);

				mem1.put(new DataItem(keyStr.getBytes()), new DataItem(valueStr.getBytes()));
			}

			SSTable.createAndRegisterNewSSTable(mem1, dataDir.getAbsolutePath(), fileAccess);

			Thread.sleep(1000);

			Memtable mem2 = Memtable.createNewMemtable(new ReentrantReadWriteLock(), dataDir.getAbsolutePath());
			for (int i = 0; i < 1000; i++) {
				String keyStr = "key-" + String.format("%03d", i);
				String valueStr = "value-value-xxxx-" + String.format("%03d", i);

				mem2.put(new DataItem(keyStr.getBytes()), new DataItem(valueStr.getBytes()));
			}

			SSTable.createAndRegisterNewSSTable(mem2, dataDir.getAbsolutePath(), fileAccess);

			for (int i = 0; i < 1000; i++) {
				String keyStr = "key-" + String.format("%03d", i);
				String valueStr = "value-value-xxxx-" + String.format("%03d", i);

				DataItem v = SSTable.find(new DataItem(keyStr.getBytes()));

				assertEquals(valueStr, new String(v.getData()));
			}
		}
	}

	@Test
	public void testIndexByteCount() throws IOException {
		Memtable mem = Memtable.createNewMemtable(new ReentrantReadWriteLock(), dataDir.getAbsolutePath());

		for (int i = 0; i < 300; i++) {
			DataItem key = new DataItem(("key-" + String.format("%03d", i)).getBytes());
			DataItem val = new DataItem(("val-" + String.format("%03d", i)).getBytes());
			mem.put(key, val);
		}

		SSTable.createAndRegisterNewSSTable(mem, dataDir.getAbsolutePath(), FileAccessChoice.SIMPLE);
		SSTable s = SSTable.sstables.get(SSTable.sstables.lastKey());

		IndexData[] indexes = s.getIndexes();

		assertEquals(indexes.length, 3);
		assertEquals(indexes[0].getByteCount(), ((7 * 2 + 4 + 4) * 128));
		assertEquals(indexes[1].getByteCount(), ((7 * 2 + 4 + 4) * 128));
		assertEquals(indexes[2].getByteCount(), ((7 * 2 + 4 + 4) * (300 - 256)));
	}
}
