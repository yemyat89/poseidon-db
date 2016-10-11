package com.poseidon.db.services;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.log4j.Logger;
import com.poseidon.db.KeyValueStore;
import com.poseidon.db.io.access.FileAccessChoice;
import com.poseidon.db.utils.DataConversion;
import com.poseidon.db.utils.IOUtils;

public class SocketServer {

	private final static Logger logger = Logger.getLogger(RequestHandler.class);
	private static final String HOSTNAME = "localhost";
	private static final int PORT = 5000;

	private static class DataServingTask implements Runnable {

		public static final byte PUT_REQUEST = 0;
		public static final byte GET_REQUEST = 1;
		public static final byte DELETE_REQUEST = 2;

		private Socket connection;
		private KeyValueStore store;
		private byte op;

		public DataServingTask(Socket connection, KeyValueStore store, byte op) {
			this.connection = connection;
			this.store = store;
			this.op = op;
		}

		public void run() {
			try {
				DataInputStream in = new DataInputStream(connection.getInputStream());
				DataOutputStream out = new DataOutputStream(connection.getOutputStream());

				if (op == PUT_REQUEST) {
					byte[] recordLengthBuf = new byte[4];
					in.read(recordLengthBuf);
					int recordLength = DataConversion.byteArrayToInt(recordLengthBuf);

					byte[] keyLengthBuf = new byte[4];
					in.read(keyLengthBuf);
					int keyLength = DataConversion.byteArrayToInt(keyLengthBuf);

					byte[] dataBuf = new byte[recordLength];
					in.read(dataBuf);

					byte[] key = Arrays.copyOfRange(dataBuf, 0, keyLength);
					byte[] value = Arrays.copyOfRange(dataBuf, keyLength, recordLength);

					boolean success = store.put(key, value);

					out.write(new byte[] { (byte) ((success) ? 1 : 0) });
					out.flush();
				} else if (op == GET_REQUEST) {
					byte[] recordLengthBuf = new byte[4];
					in.read(recordLengthBuf);
					int recordLength = DataConversion.byteArrayToInt(recordLengthBuf);

					byte[] dataBuf = new byte[recordLength];
					in.read(dataBuf);

					byte[] key = Arrays.copyOfRange(dataBuf, 0, recordLength);
					byte[] value = store.get(key);

					if (value == null) {
						out.write(DataConversion.intToByteArray(0));
						out.flush();
					} else {
						byte[] respLengthBuf = DataConversion.intToByteArray(value.length);
						byte[] responseData = IOUtils.concatByteArrays(respLengthBuf, value);

						out.write(responseData);
						out.flush();
					}
				} else if (op == DELETE_REQUEST) {
					byte[] recordLengthBuf = new byte[4];
					in.read(recordLengthBuf);
					int recordLength = DataConversion.byteArrayToInt(recordLengthBuf);

					byte[] dataBuf = new byte[recordLength];
					in.read(dataBuf);

					byte[] key = Arrays.copyOfRange(dataBuf, 0, recordLength);

					boolean success = store.delete(key);

					out.write(new byte[] { (byte) ((success) ? 1 : 0) });
					out.flush();
				} else {
					logger.warn("Unknown Resquest");
				}

				out.close();
				in.close();
				connection.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private KeyValueStore store;
	private ExecutorService es;

	public SocketServer(KeyValueStore store, FileAccessChoice fileAccessChoice) throws IOException {
		this.store = store;
		es = Executors.newFixedThreadPool(50);
	}

	public void start() {

		(new Thread(() -> {

			store.start();

			try (ServerSocket server = new ServerSocket(PORT)) {
				logger.info("Socket Server is running at " + HOSTNAME + ":" + PORT);

				while (true) {
					Socket connection = server.accept();

					DataInputStream in = new DataInputStream(connection.getInputStream());
					byte[] opBuf = new byte[1];
					in.read(opBuf);

					if (opBuf[0] == RequestHandler.SHUTDOWN_REQUEST) {
						connection.close();
						break;
					} else {
						es.submit(new DataServingTask(connection, store, opBuf[0]));
					}
				}
			} catch (IOException e) {
				logger.error("Failed during accepting request - " + e);
			}

		})).start();
	}

	public void stop(boolean force) {
		if (force) {
			es.shutdownNow();
		} else {
			es.shutdown();
		}

		try (Socket socket = new Socket(HOSTNAME, PORT)) {
			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
			out.write(new byte[] { RequestHandler.SHUTDOWN_REQUEST });
			out.flush();
		} catch (IOException e) {
			logger.error("Failed to send shutdown request to Socket Server");
		}
	}
}
