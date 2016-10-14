package com.poseidon.db.utils;

public class RESTUtils {
	public static class DataFormat {
		public static class GetResult {
			public String result;
		}

		public static class PostRequest {
			public String key;
			public String value;
		}

		public static class DeleteRequest {
			public String key;
		}

		public static class UpdateResult {
			public boolean success;
		}
	}
}
