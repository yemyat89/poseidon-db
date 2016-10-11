package com.poseidon.db.utils;

public class DataConversion {
	public static byte[] intToByteArray(int i) {
		return new byte[] {
			(byte)(i >>> 24), (byte)(i >>> 16), (byte)(i >>> 8), (byte) i 
	    };
	}
	
	public static byte[] longToByteArray(long i) {
		return new byte[] {
			(byte)(i >>> 56), (byte)(i >>> 48), (byte)(i >>> 40), (byte)(i >>> 32),
			(byte)(i >>> 24), (byte)(i >>> 16), (byte)(i >>> 8), (byte) i 
	    };
	}
	
	public static int byteArrayToInt(byte[] bytes) {
	     return (bytes[0] << 24 | (bytes[1] & 0xFF) << 16 | 
	    		 (bytes[2] & 0xFF) << 8 | (bytes[3] & 0xFF));
	}
	
	public static int byteArrayToLong(byte[] bytes) {
	     return (
	    		 bytes[0] << 56 | (bytes[1] & 0xFF) << 48 | 
	    		 (bytes[2] & 0xFF) << 40 | (bytes[3] & 0xFF) << 32 |
	    		 (bytes[4] & 0xFF) << 24 | (bytes[5] & 0xFF) << 16 | 
	    		 (bytes[6] & 0xFF) << 8 | (bytes[7] & 0xFF)
    		 );
	}
}
