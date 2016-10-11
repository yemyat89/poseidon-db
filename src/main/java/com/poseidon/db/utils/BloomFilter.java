package com.poseidon.db.utils;

import java.util.BitSet;

public class BloomFilter {
	
	private BitSet filter;
	
	public BloomFilter() {
		filter = new BitSet();
	}
	
	public BloomFilter(byte[] data) {
		filter = BitSet.valueOf(data);
	}
	
	public void add(byte[] data) {
		filter.or(BitSet.valueOf(data));
	}
	
	public boolean contains(byte[] data) {
		BitSet target = BitSet.valueOf(data);
		target.and(filter);
		return target.equals(BitSet.valueOf(data));
	}
	
	public byte[] getByteArray() {
		return filter.toByteArray();
	}
}
