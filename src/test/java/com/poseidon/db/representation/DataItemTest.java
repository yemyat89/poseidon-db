package com.poseidon.db.representation;

import static org.junit.Assert.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.stream.IntStream;
import org.junit.Test;
import com.poseidon.db.representation.DataItem;

public class DataItemTest {

	@Test
	public void testDataItemReturnWrappedByteArray() {
		final byte[] data = new byte[] { 10, 23, 73, 12 };
		final DataItem dt = new DataItem(data);
		final byte[] returnedData = dt.getData();

		IntStream.range(0, data.length).forEach((i) -> {
			assertEquals(data[i], returnedData[i]);
		});
	}

	@Test
	public void testDataItemEqualsHash() {
		byte[] data1 = new byte[] { 10, 23, 73, 12 };
		DataItem dt1 = new DataItem(data1);

		byte[] data2 = new byte[] { 80, 23, 74, 12 };
		DataItem dt2 = new DataItem(data2);

		byte[] data3 = new byte[] { 10, 23, 73, 12 };
		DataItem dt3 = new DataItem(data3);

		byte[] data4 = new byte[] { 80, 23, 74, 12 };
		DataItem dt4 = new DataItem(data4);

		assertEquals(dt1, dt3);
		assertEquals(dt2, dt4);

		HashMap<DataItem, String> map = new HashMap<DataItem, String>();
		map.put(dt1, "Put-By-Data-Item-1");
		map.put(dt2, "Put-By-Data-Item-2");
		assertEquals(map.get(dt3), map.get(dt1));
		assertEquals(map.get(dt4), map.get(dt2));
	}

	@Test
	public void testDataItemSort() {
		String[] data = new String[] { "ballxssx", "aaaaaaaaaaaaaa", "pop", "zooo", "dab", "ax", "najdsa", };

		DataItem[] ddata = new DataItem[data.length];

		for (int i = 0; i < ddata.length; i++) {
			ddata[i] = new DataItem(data[i].getBytes());
		}

		Arrays.sort(data);
		Arrays.sort(ddata);

		for (int i = 0; i < ddata.length; i++) {
			assertEquals(data[i], new String(ddata[i].getData()));
		}
	}

}
