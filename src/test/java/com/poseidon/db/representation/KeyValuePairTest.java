package com.poseidon.db.representation;

import static org.junit.Assert.*;
import java.util.HashMap;
import org.junit.Test;
import com.poseidon.db.representation.DataItem;
import com.poseidon.db.representation.KeyValuePair;

public class KeyValuePairTest {

	@Test
	public void testKVPHasValueMutable() {
		DataItem originalKey = new DataItem("original-key".getBytes());
		DataItem originalValue = new DataItem("original-value".getBytes());
		KeyValuePair kvp = new KeyValuePair(originalKey, originalValue);
		DataItem newValue = new DataItem("new-value".getBytes());
		kvp.setValue(newValue);
		assertEquals(newValue, kvp.getValue());

		DataItem anotherKey = new DataItem("another-key".getBytes());
		DataItem anotherValue = new DataItem("another-value".getBytes());
		KeyValuePair kvp2 = new KeyValuePair(anotherKey, anotherValue);

		kvp.setNext(kvp2);
		assertNotEquals(kvp.getNext(), null);
	}

	@Test
	public void testKVPEqualsAndHash() {
		DataItem key1 = new DataItem("key1".getBytes());
		DataItem value1 = new DataItem("value1".getBytes());
		KeyValuePair kvp1 = new KeyValuePair(key1, value1);
		KeyValuePair kvp2 = new KeyValuePair(key1, value1);
		assertEquals(kvp1, kvp2);

		HashMap<KeyValuePair, String> map = new HashMap<KeyValuePair, String>();
		map.put(kvp1, "put-by-kvp1");
		assertEquals(map.get(kvp2), map.get(kvp1));
	}

}
