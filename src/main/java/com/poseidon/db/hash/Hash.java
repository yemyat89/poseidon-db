package com.poseidon.db.hash;

import com.poseidon.db.representation.DataItem;

public interface Hash {
	int calculateHash(DataItem key);
	void setCapacity(int newCapacity);
	int getCapacity();
}