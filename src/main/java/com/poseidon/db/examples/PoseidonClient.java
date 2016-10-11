package com.poseidon.db.examples;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import org.apache.log4j.Logger;
import com.poseidon.db.utils.DataConversion;
import com.poseidon.db.utils.IOUtils;

public class PoseidonClient {

	private final static Logger logger = Logger.getLogger(PoseidonClient.class);
	public static final int DEFAULT_NUM_OF_RETRY = 3;
	public static final int DEFAULT_WAIT_BEFORE_RETRY = 3;

	@FunctionalInterface
	interface Function {
		public Object call(byte[] key, byte[] value);
	}

	private String host;
	private int port;
	private int numOfRetry;
	private int waitBeforeRetry;

	public PoseidonClient(String host, int port) {
		this(host, port, DEFAULT_NUM_OF_RETRY, DEFAULT_WAIT_BEFORE_RETRY);
	}

	public PoseidonClient(String host, int port, int numOfRetry, int waitBeforeRetry) {
		this.host = host;
		this.port = port;
		this.numOfRetry = numOfRetry;
		this.waitBeforeRetry = waitBeforeRetry;
	}

	public byte[] get(byte[] key) {
		return (byte[]) retryLoop(this::getData, key, null);
	}

	public boolean put(byte[] key, byte[] value) {
		return (boolean) retryLoop(this::putData, key, value);
	}

	public boolean delete(byte[] key) {
		return (boolean) retryLoop(this::deleteData, key, null);
	}

	private Object retryLoop(Function func, byte[] key, byte[] value) {
		for (int i = 0; i < numOfRetry;) {
			try {
				return func.call(key, value);
			} catch (Exception e) {
				i++;
				if (i == numOfRetry) {
					throw e;
				}
				try {
					Thread.sleep(waitBeforeRetry * 1000);
				} catch (InterruptedException e1) {
					// No need to handle, just retry immediately
				}
			}
		}
		return null;
	}

	private Object getData(byte[] key, byte[] _dummy) {
		byte[] value = null;

		try (Socket socket = new Socket(host, port)) {
			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
			DataInputStream in = new DataInputStream(socket.getInputStream());

			byte[] opBuf = new byte[] { 1 };
			byte[] keyLengthBuffer = DataConversion.intToByteArray(key.length);

			byte[] data = IOUtils.concatByteArrays(opBuf, keyLengthBuffer, key);
			out.write(data);
			out.flush();

			byte[] respLengthBuf = new byte[4];
			in.read(respLengthBuf);
			int respLength = DataConversion.byteArrayToInt(respLengthBuf);
			value = new byte[respLength];
			in.read(value);

			socket.close();
			out.close();
			in.close();
		} catch (IOException e) {
			logger.debug("Failed to get data, there may be a retry");
		}
		return (value == null || value.length == 0) ? null : value;
	}

	private Object putData(byte[] key, byte[] value) {
		try (Socket socket = new Socket(host, port)) {
			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
			DataInputStream in = new DataInputStream(socket.getInputStream());

			byte[] opBuf = new byte[] { 0 };
			byte[] recordLengthBuf = DataConversion.intToByteArray(key.length + value.length);
			byte[] keyLengthBuffer = DataConversion.intToByteArray(key.length);

			byte[] data = IOUtils.concatByteArrays(opBuf, recordLengthBuf, keyLengthBuffer, key, value);
			out.write(data);
			out.flush();

			byte[] successBuf = new byte[1];
			in.read(successBuf);

			socket.close();
			out.close();
			in.close();

			return (successBuf[0] == 1);
		} catch (IOException e) {
			logger.debug("Failed to put data, there may be a retry");
		}
		return false;
	}

	private Object deleteData(byte[] key, byte[] _dummy) {
		try (Socket socket = new Socket(host, port)) {
			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
			DataInputStream in = new DataInputStream(socket.getInputStream());

			byte[] opBuf = new byte[] { 2 };
			byte[] keyLengthBuffer = DataConversion.intToByteArray(key.length);

			byte[] data = IOUtils.concatByteArrays(opBuf, keyLengthBuffer, key);
			out.write(data);
			out.flush();

			byte[] successBuf = new byte[1];
			in.read(successBuf);

			socket.close();
			out.close();
			in.close();

			return (successBuf[0] == 1);
		} catch (IOException e) {
			logger.debug("Failed to delete data, there may be a retry");
		}
		return false;
	}
}
