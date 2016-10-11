package com.poseidon.db.representation;

import java.util.Arrays;

public class DataItem implements Comparable<DataItem> {
	private byte[] data;

	public DataItem(byte[] data) {
		this.data = data;
	}

	public byte[] getData() {
		return data;
	}

	public int length() {
		return data.length;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof DataItem)) {
			return false;
		}
		return (compareTo(((DataItem) obj)) == 0);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(data);
	}

	@Override
	public int compareTo(DataItem other) {
		int n = Math.min(data.length, other.getData().length);

		for (int i = 0, j = 0; i < n; i++, j++) {
			byte v1 = data[i];
			byte v2 = other.getData()[j];

			if (v1 == v2) {
				continue;
			} else if (v2 > v1) {
				return -1;
			} else {
				return 1;
			}
		}

		return (data.length - n) - (other.getData().length - n);
	}
}
