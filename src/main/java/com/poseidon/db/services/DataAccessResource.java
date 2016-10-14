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
import com.poseidon.db.utils.RESTUtils.DataFormat.DeleteRequest;
import com.poseidon.db.utils.RESTUtils.DataFormat.GetResult;
import com.poseidon.db.utils.RESTUtils.DataFormat.PostRequest;
import com.poseidon.db.utils.RESTUtils.DataFormat.UpdateResult;

@Path(RESTServer.DATA_ACCESS_PATH)
public class DataAccessResource {

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public GetResult getValue(@Context ServletContext servletContext, @QueryParam("key") String key) {

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
	public UpdateResult putKeyValue(@Context ServletContext servletContext, PostRequest data) {

		KeyValueStore store = (KeyValueStore) servletContext.getAttribute("kvstore");

		UpdateResult postDeleteRequest = new UpdateResult();
		postDeleteRequest.success = store.put(data.key.getBytes(), data.value.getBytes());

		return postDeleteRequest;
	}

	@DELETE
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public UpdateResult deleteKeyValue(@Context ServletContext servletContext, DeleteRequest data) {

		KeyValueStore store = (KeyValueStore) servletContext.getAttribute("kvstore");

		UpdateResult postDeleteResult = new UpdateResult();
		postDeleteResult.success = store.delete(data.key.getBytes());

		return postDeleteResult;
	}
}