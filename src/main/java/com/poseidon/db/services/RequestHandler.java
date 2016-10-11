package com.poseidon.db.services;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import org.apache.log4j.Logger;
import com.poseidon.db.KeyValueStore;
import com.poseidon.db.io.access.FileAccessChoice;

public class RequestHandler {

	private final static Logger logger = Logger.getLogger(RequestHandler.class);
	public static final String FILE_ACCESS_CHOICE_ARG_NAME = "fileaccess";
	public static final String FILE_ACCESS_CHOICE_SIMPLE = "simple";
	public static final String FILE_ACCESS_CHOICE_MEM_MAP = "mem-map";
	public static final byte SHUTDOWN_REQUEST = 127;
	public static final byte SHUTDOWN_REQUEST_FORCE = 126;
	private static final int PORT = 5010;

	private KeyValueStore store;
	private SocketServer socketServer;
	private RESTServer restServer;

	public RequestHandler(String dataDir, FileAccessChoice fileAccessChoice) throws IOException {
		store = KeyValueStore.getNewInstance(dataDir, fileAccessChoice);
		socketServer = new SocketServer(store, fileAccessChoice);
		restServer = new RESTServer(store);
	}

	public void start() {

		socketServer.start();
		restServer.start();

		try (ServerSocket server = new ServerSocket(PORT)) {
			while (true) {
				Socket connection = server.accept();

				DataInputStream in = new DataInputStream(connection.getInputStream());
				byte[] opBuf = new byte[1];
				in.read(opBuf);

				if (opBuf[0] == SHUTDOWN_REQUEST) {
					logger.info("Shutting down service now");
					stop(false);
					break;
				} else if (opBuf[0] == SHUTDOWN_REQUEST_FORCE) {
					logger.info("Shutting down service now");
					stop(true);
					break;
				}
			}
		} catch (IOException e) {
			logger.error("Failed to terminate the datastore process");
			e.printStackTrace();
		}
	}

	public void stop(boolean force) {
		store.stop(force);
		socketServer.stop(force);
		restServer.stop();
	}

	public static void main(String[] args) throws IOException {

		if (args.length < 1) {
			logger.info("Please provide data path");
			System.exit(1);
		}

		String dataDir = args[0];
		String fileAccessChoiceStr = System.getProperty(FILE_ACCESS_CHOICE_ARG_NAME);
		FileAccessChoice fileAccessChoice = FileAccessChoice.SIMPLE;

		if (fileAccessChoiceStr != null) {
			if (fileAccessChoiceStr.equals(FILE_ACCESS_CHOICE_SIMPLE)) {
				logger.info("Using simple no buffer");
				fileAccessChoice = FileAccessChoice.SIMPLE;
			} else if (fileAccessChoiceStr.equals(FILE_ACCESS_CHOICE_MEM_MAP)) {
				logger.info("Using memory-mapped file");
				fileAccessChoice = FileAccessChoice.MEM_MAP;
			}
		}

		(new RequestHandler(dataDir, fileAccessChoice)).start();
	}
}
