package com.poseidon.db.hash;

import com.poseidon.db.representation.DataItem;

public class SimpleModuloHash implements Hash {
	
	private int capacity;
	
	public SimpleModuloHash(int capacity) {
		this.capacity = capacity;
	}

	@Override
	public int calculateHash(DataItem key) {
		int hashCode = key.hashCode() & 0x7fffffff;
		return (hashCode ^ hashCode >>> 16) % capacity;
	}

	@Override
	public void setCapacity(int newCapacity) {
		capacity = newCapacity;
	}

	@Override
	public int getCapacity() {
		return capacity;
	}

}
