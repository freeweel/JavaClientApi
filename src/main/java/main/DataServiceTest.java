package main;

import java.io.File;
import java.io.Reader;
import java.util.logging.Logger;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;

import org.apache.commons.io.IOUtils;

import config.Config;
import dataservices.Test;

/**
 * Used to test MarkLogic's Data Services feature
 */
public final class DataServiceTest {
	// private static Config config = Config.getConfig(ENV.dev);
	private static Logger logger = Logger.getLogger("test");


	/**
	 * Main method to get a MarkLogic connection and call the Test data service
	 */
	public static void main(String[] args) throws Exception {
		Config config = Config.setFile(new File("src/main/resources/AccessKeys.secret"));

		String host = config.getMLHost();
		int FINAL_PORT = config.getMLFinalDbPort();
		String userName = config.getMLUser();
		String password = config.getMLPassword();

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