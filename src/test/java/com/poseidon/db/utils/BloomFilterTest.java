package com.poseidon.db.utils;

import static org.junit.Assert.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.poseidon.db.utils.BloomFilter;

public class BloomFilterTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testBloomFilter() {
		BloomFilter b = new BloomFilter();
		for (int i = 0; i < 1000000; i++) {
			String key = "key-" + i;
			b.add(key.getBytes());
		}

		for (int i = 0; i < 1000000; i++) {
			String key = "key-" + i;
			assertTrue(b.contains(key.getBytes()));
		}
	}

}
