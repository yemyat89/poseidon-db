package com.poseidon.db.client;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.poseidon.db.utils.RESTUtils.DataFormat.DeleteRequest;
import com.poseidon.db.utils.RESTUtils.DataFormat.GetResult;
import com.poseidon.db.utils.RESTUtils.DataFormat.PostRequest;
import com.poseidon.db.utils.RESTUtils.DataFormat.UpdateResult;

public class PoseidonRESTClient implements PoseidonClient {
	
	private String baseUrl;
	
	public PoseidonRESTClient(String baseUrl) {
		this.baseUrl = baseUrl;
	}

	@Override
	public byte[] get(byte[] key) throws IOException {

		HttpURLConnection connection = null;
		byte[] value = null;

		try {
			String keyStr = new String(key);
			URL url = new URL(baseUrl + "?key=" + keyStr);
			connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("GET");
			connection.setRequestProperty("Accept", "application/json");

			if (connection.getResponseCode() != 200) {
				throw new RuntimeException("Failed : HTTP error code : " + connection.getResponseCode());
			}

			DataInputStream in = new DataInputStream(connection.getInputStream());
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			byte[] buf = new byte[4096];
			int readCount = in.read(buf);

			while (readCount > 0) {
				baos.write(buf, 0, readCount);
				readCount = in.read(buf);
			}

			String response = new String(baos.toByteArray());
			ObjectMapper mapper = new ObjectMapper();
			GetResult getResult = mapper.readValue(response, GetResult.class);

			value = getResult.result.getBytes();

		} finally {
			if (connection != null)
				connection.disconnect();
		}

		return (value == null || value.length == 0) ? null : value;
	}

	@Override
	public boolean put(byte[] key, byte[] value) throws IOException {

		HttpURLConnection connection = null;
		boolean success = false;

		try {
			URL url = new URL(baseUrl);
			connection = (HttpURLConnection) url.openConnection();

			connection.setDoInput(true);
			connection.setDoOutput(true);
			connection.setRequestMethod("POST");
			connection.setRequestProperty("Accept", "application/json");
			connection.setRequestProperty("Content-Type", "application/json; charset=UTF-8");

			PostRequest requestBody = new PostRequest();
			requestBody.key = new String(key);
			requestBody.value = new String(value);
			ObjectMapper mapper = new ObjectMapper();

			OutputStreamWriter writer = new OutputStreamWriter(connection.getOutputStream(), "UTF-8");
			writer.write(mapper.writeValueAsString(requestBody));
			writer.close();

			if (connection.getResponseCode() != 200) {
				throw new RuntimeException("Failed : HTTP error code : " + connection.getResponseCode());
			}

			BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			StringBuffer jsonString = new StringBuffer();
			String line;
			while ((line = br.readLine()) != null) {
				jsonString.append(line);
			}
			br.close();

			UpdateResult updateResult = mapper.readValue(jsonString.toString(), UpdateResult.class);
			success = updateResult.success;

		} finally {
			if (connection != null)
				connection.disconnect();
		}

		return success;
	}

	@Override
	public boolean delete(byte[] key) throws IOException {
		
		HttpURLConnection connection = null;
		boolean success = false;

		try {
			URL url = new URL(baseUrl);
			connection = (HttpURLConnection) url.openConnection();

			connection.setDoInput(true);
			connection.setDoOutput(true);
			connection.setRequestMethod("DELETE");
			connection.setRequestProperty("Accept", "application/json");
			connection.setRequestProperty("Content-Type", "application/json; charset=UTF-8");

			DeleteRequest requestBody = new DeleteRequest();
			requestBody.key = new String(key);
			ObjectMapper mapper = new ObjectMapper();

			OutputStreamWriter writer = new OutputStreamWriter(connection.getOutputStream(), "UTF-8");
			writer.write(mapper.writeValueAsString(requestBody));
			writer.close();

			if (connection.getResponseCode() != 200) {
				throw new RuntimeException("Failed : HTTP error code : " + connection.getResponseCode());
			}

			BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			StringBuffer jsonString = new StringBuffer();
			String line;
			while ((line = br.readLine()) != null) {
				jsonString.append(line);
			}
			br.close();

			UpdateResult updateResult = mapper.readValue(jsonString.toString(), UpdateResult.class);
			success = updateResult.success;

		} finally {
			if (connection != null)
				connection.disconnect();
		}

		return success;
	}
}
