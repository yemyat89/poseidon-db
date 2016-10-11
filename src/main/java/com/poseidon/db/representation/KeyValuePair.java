package com.poseidon.db.representation;

import java.io.DataOutput;
import java.io.IOException;
import com.poseidon.db.utils.DataConversion;

public class KeyValuePair implements Comparable<KeyValuePair> {
	private DataItem key;
	private DataItem value;
	private KeyValuePair next;

	public KeyValuePair(DataItem key, DataItem value) {
		this.key = key;
		this.value = value;
		this.next = null;
	}

	public DataItem getKey() {
		return key;
	}

	public DataItem getValue() {
		return value;
	}

	public void setValue(DataItem value) {
		this.value = value;
	}

	public KeyValuePair getNext() {
		return next;
	}

	public void setNext(KeyValuePair next) {
		this.next = next;
	}

	public void writeToFile(DataOutput out) throws IOException {
		int recordLength = key.length() + value.length();
		int keyLength = key.length();

		out.write(DataConversion.intToByteArray(recordLength));
		out.write(DataConversion.intToByteArray(keyLength));
		out.write(key.getData());
		out.write(value.getData());
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof KeyValuePair)) {
			return false;
		}
		KeyValuePair other = (KeyValuePair) obj;
		return key.equals(other.getKey()) && value.equals(other.getValue());
	}

	@Override
	public int hashCode() {
		return (key.hashCode() ^ value.hashCode());
	}

	@Override
	public int compareTo(KeyValuePair other) {
		return key.compareTo(other.getKey());
	}
}