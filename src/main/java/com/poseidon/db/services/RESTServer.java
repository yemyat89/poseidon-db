package com.poseidon.db.services;

import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import com.poseidon.db.KeyValueStore;

public class RESTServer {

	private final static Logger logger = Logger.getLogger(RESTServer.class);
	public static final String DATA_ACCESS_PATH = "data-access";
	public static final String HOSTNAME = "localhost";
	public static final int PORT = 5050;

	private Server server;
	private KeyValueStore store;

	public RESTServer(KeyValueStore store) {
		this.store = store;
	}

	public void start() {
		ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
		context.setContextPath("/");

		server = new Server(5050);
		server.setHandler(context);

		ServletHolder jerseyServlet = context.addServlet(org.glassfish.jersey.servlet.ServletContainer.class, "/*");
		jerseyServlet.setInitOrder(0);

		context.setAttribute("kvstore", store);

		jerseyServlet.setInitParameter("jersey.config.server.provider.classnames",
				DataAccessResource.class.getCanonicalName());
		try {
			server.start();
			logger.info("REST Server is running at " + HOSTNAME + ":" + PORT);
		} catch (Exception e) {
			logger.error("Failed to start REST server");
		}
	}

	public void stop() {
		try {
			server.stop();
		} catch (Exception e) {
			logger.error("Failed to stop REST server");
		}
	}
}
