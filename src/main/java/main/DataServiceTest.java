package main;

import java.io.Reader;
import java.util.logging.Logger;

import org.apache.commons.io.IOUtils;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;

import config.Config;
import config.Config.ENV;
import dataservices.Test;

/**
 * 
 */
public final class DataServiceTest {
	private static Config config = Config.getConfig(ENV.local);
	// private static Config config = Config.getConfig(ENV.dev);
	private static Logger logger = Logger.getLogger("test");


	/**
	 * Main method to get a MarkLogic connection and call the Test data service
	 */
	public static void main(String[] args) {
		String host = config.server();
		int FINAL_PORT = config.portFinal();
		String userName = config.user();
		String password = config.pwd();

		try {
			final DatabaseClient dbClient = DatabaseClientFactory.newClient(host, FINAL_PORT,
					new DatabaseClientFactory.BasicAuthContext(userName, password));
			
			// Initialize the service and run an endpoint
			Test test = Test.on(dbClient);
			Reader reader = test.hello("Natasha Romanoff");

			// Log the response
			logger.fine("Result: " + IOUtils.toString(reader));
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}