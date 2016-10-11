package com.poseidon.db.services;

import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import com.poseidon.db.KeyValueStore;

@Path("data-access")
public class DataAccessResource {

	static class GetResult {
		public String result;
	}

	static class PostRequest {
		public String key;
		public String value;
	}

	static class DeleteRequest {
		public String key;
	}

	static class PostDeleteResult {
		public boolean success;
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public GetResult findKey(@Context ServletContext servletContext, @QueryParam("key") String key) {

		KeyValueStore store = (KeyValueStore) servletContext.getAttribute("kvstore");
		byte[] value = store.get(key.getBytes());

		GetResult result = new GetResult();

		if (value != null) {
			result.result = new String(value);
		} else {
			result.result = "";
		}
		return result;
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public PostDeleteResult createOrders(@Context ServletContext servletContext, PostRequest data) {

		KeyValueStore store = (KeyValueStore) servletContext.getAttribute("kvstore");

		PostDeleteResult postDeleteRequest = new PostDeleteResult();
		postDeleteRequest.success = store.put(data.key.getBytes(), data.value.getBytes());

		return postDeleteRequest;
	}

	@DELETE
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public PostDeleteResult deleteKey(@Context ServletContext servletContext, DeleteRequest data) {

		KeyValueStore store = (KeyValueStore) servletContext.getAttribute("kvstore");

		PostDeleteResult postDeleteResult = new PostDeleteResult();
		postDeleteResult.success = store.delete(data.key.getBytes());

		return postDeleteResult;
	}
}