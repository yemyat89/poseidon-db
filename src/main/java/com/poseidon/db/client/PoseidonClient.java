package com.poseidon.db.client;

import java.io.IOException;

public interface PoseidonClient {
	public byte[] get(byte[] key) throws IOException;
	
	public boolean put(byte[] key, byte[] value) throws IOException;
	
	public boolean delete(byte[] key) throws IOException;
}
