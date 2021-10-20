package main;

import java.io.File;
import java.time.Instant;
import java.util.logging.Logger;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.JobTicket;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.document.ServerTransform;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.FileHandle;

import config.Config;
import config.Config.ENV;

/**
 * This class is a simple test to make sure it is possible to create documents
 * in MarkLogic from the DatabaseClient libraries.
 * 
 * @author Mark Dyrhaug
 * @since 05/04/2020
 *
 */
public final class CoordinateWriteBatcher {
	private static Config config = Config.getConfig(ENV.local);
	//private static Config config = Config.getConfig(ENV.dev);

	// private static Logger logger =
	// LogManager.getLogManager().getLogger(CoordinateWriteBatcher.class.getCanonicalName());
	private static Logger logger = Logger.getLogger("test");

	/**
	 * This is a utility class and prevents the default parameter-less constructor
	 * from being used elsewhere.
	 */
	private CoordinateWriteBatcher() {
		// not called.
	}

	/**
	 * This method is for command-line execution. This uses the MarkLogic
	 * DataMovement SDK to create a document in MarkLogic.
	 * 
	 * @param args - 1 - Host 2 - UserName (For MarkLogic) 3 - Password (For
	 *             MarkLogic)
	 */
	public static void main(String[] args) {
		
		String host = config.server();
		int STAGING_PORT = config.portStaging();
		String userName = config.user();
		String password = config.pwd();
		String flowName = "DMSDK-Test";
		String jobId = "PVTEST";

		try {

			// Authorization Context can be used with all databases involved in the flow
			// execution.
//			DatabaseClientFactory.BasicAuthContext authContext = new DatabaseClientFactory.BasicAuthContext(userName, password);
//			DatabaseClientFactory.SSLHostnameVerifier verifier = DatabaseClientFactory.SSLHostnameVerifier.ANY;			
//			authContext.withSSLHostnameVerifier(verifier);

			// Staging Database Client.
			// DatabaseClient stagingDatabaseClient = DatabaseClientFactory.newClient(host,
			// STAGING_PORT, authContext.withSSLContext(SSLContext.getDefault(),
			// authContext.getTrustManager()));
			final DatabaseClient stagingDatabaseClient = DatabaseClientFactory.newClient(host, STAGING_PORT,
					new DatabaseClientFactory.BasicAuthContext(userName, password));

			final DataMovementManager dataMovementManager = stagingDatabaseClient.newDataMovementManager();
			DocumentMetadataHandle dmdh = new DocumentMetadataHandle();
			dmdh.withMetadataValue("datahubCreatedBy", userName);
			dmdh.withMetadataValue("datahubCreatedByStep", "");
			dmdh.withMetadataValue("datahubCreatedInFlow", flowName);
			dmdh.withMetadataValue("datahubCreatedOn", Instant.now().toString());
			dmdh.withMetadataValue("datahubCreatedByJob", jobId);
			dmdh.withCollections(flowName + "-Ingest");
			
			FileHandle doc1 = new FileHandle(new File("/dev/aha/PaulTest.pdf"));
			FileHandle doc2 = new FileHandle(new File("/wps-ingest-data/TOPS-Providers/organization_example.xml"));
			
			ServerTransform dhfTransform = new ServerTransform("hub-ingest");

			WriteBatcher whb = dataMovementManager.newWriteBatcher();
			whb.withBatchSize(1);
			whb.withThreadCount(1);
			//whb.withTransform(dhfTransform);
			whb.onBatchSuccess(batch -> {
				logger.info("batch # " + batch.getJobBatchNumber() + " so far: " + batch.getJobWritesSoFar());
			});
			whb.onBatchFailure((batch, throwable) -> throwable.printStackTrace());
			whb.add("/PVTEST/PaulTest.pdf", dmdh, doc1);
			//whb.add("/PVTEST/Test2Xml.json", dmdh, doc2);

			JobTicket ticket = dataMovementManager.startJob(whb);
			whb.flushAndWait(); // send the two docs
			dataMovementManager.stopJob(ticket);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}