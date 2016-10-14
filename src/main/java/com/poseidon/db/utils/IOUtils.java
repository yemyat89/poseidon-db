package com.poseidon.db.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class IOUtils {
	
	public static byte[] getSubByteArray(byte[] data, int start, int end) {
		end = Math.min(end, data.length);
		return Arrays.copyOfRange(data, start, end);
	}
	
	public static byte[] concatByteArrays(byte[]... bytes) throws IOException {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream( );
		
		for (byte[] b: bytes) {
			outputStream.write(b);
		}
		
		return outputStream.toByteArray();
	}
}
