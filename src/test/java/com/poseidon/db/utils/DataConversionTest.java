package com.poseidon.db.utils;

import static org.junit.Assert.*;
import org.junit.Test;
import com.poseidon.db.utils.DataConversion;

public class DataConversionTest {

	@Test
	public void testConversions() {
		int max = 1000000, min = -1000000;

		for (int i = 0; i < max; i++) {
			byte[] b = DataConversion.intToByteArray(i);
			assertEquals(i, DataConversion.byteArrayToInt(b));
		}
		for (int i = max; i < min; i--) {
			byte[] b = DataConversion.intToByteArray(i);
			assertEquals(i, DataConversion.byteArrayToInt(b));
		}
		for (long i = 0; i < max; i++) {
			byte[] b = DataConversion.longToByteArray(i);
			assertEquals(i, DataConversion.byteArrayToLong(b));
		}
		for (long i = max; i < min; i--) {
			byte[] b = DataConversion.longToByteArray(i);
			assertEquals(i, DataConversion.byteArrayToLong(b));
		}
	}

}
