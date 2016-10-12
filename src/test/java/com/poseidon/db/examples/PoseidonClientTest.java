package com.poseidon.db.examples;

import static org.junit.Assert.*;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.poseidon.db.TestUtils;
import com.poseidon.db.examples.PoseidonClient;
import com.poseidon.db.io.access.FileAccessChoice;
import com.poseidon.db.services.RequestHandler;

public class PoseidonClientTest {

	private File dataDir;

	@Before
	public void setUp() throws Exception {
		dataDir = new File("/tmp/geez");
		dataDir.mkdir();
	}

	@After
	public void tearDown() throws Exception {
		TestUtils.deleteFolder(dataDir);
	}

	@Test
	public void testClient() throws IOException {
		Thread server = new Thread(() -> {
			try {
				RequestHandler r = new RequestHandler(dataDir.getAbsolutePath(), FileAccessChoice.MEM_MAP);
				r.start();
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
		server.start();

		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		PoseidonClient client = new PoseidonClient("localhost", 5000);

		String data = "Advantage old had otherwise sincerity dependent additions. "
				+ "It in adapted natural hastily is justice. Six draw you him full "
				+ "not mean evil. Prepare garrets it expense windows shewing do an. "
				+ "She projection advantages resolution son indulgence. Part sure "
				+ "on no long life am at ever. In songs above he as drawn to. Gay was "
				+ "outlived peculiar rendered led six. ";

		String[] keywords = data.split("\\s");

		for (String keyword : keywords) {
			byte[] key = keyword.getBytes();
			byte[] value = (keyword + "-#######-" + keyword + "-#######-" + keyword).getBytes();
			client.put(key, value);
		}

		for (String keyword : keywords) {
			byte[] key = keyword.getBytes();
			byte[] value = client.get(key);
			byte[] expectedValue = (keyword + "-#######-" + keyword + "-#######-" + keyword).getBytes();
			assertTrue(Arrays.equals(expectedValue, value));
		}

		client.delete(keywords[10].getBytes());
		assertEquals(client.get(keywords[10].getBytes()), null);
	}

}
