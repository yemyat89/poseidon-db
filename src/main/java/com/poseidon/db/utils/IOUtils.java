package com.poseidon.db.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class IOUtils {
	public static byte[] concatByteArrays(byte[]... bytes) throws IOException {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream( );
		
		for (byte[] b: bytes) {
			outputStream.write(b);
		}
		
		return outputStream.toByteArray();
	}
}
