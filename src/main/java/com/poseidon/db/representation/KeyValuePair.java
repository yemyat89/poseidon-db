package com.poseidon.db.representation;

import java.io.DataOutput;
import java.io.IOException;
import com.poseidon.db.utils.DataConversion;
import com.poseidon.db.utils.IOUtils;

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

		byte[] recordLengthData = DataConversion.intToByteArray(recordLength);
		byte[] keyLengthData = DataConversion.intToByteArray(keyLength);
		byte[] dataToWrite = IOUtils.concatByteArrays(recordLengthData, keyLengthData, key.getData(), value.getData());

		out.write(dataToWrite);
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