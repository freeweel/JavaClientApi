package main;

import java.io.InputStream;
import java.time.Instant;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.DigestAuthContext;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.io.DocumentMetadataHandle;
import config.Config;

/**
 * This class provides a layer around the Data Movement SDK's
 * DataMovementManager and WriteBatcher
 */
public class MarkLogicDataMovement {
	private DataMovementManager manager;

	/**
	 * Constructor
	 * 
	 * @param hostname The name or IP address of the host
	 * @param port     The database service port
	 * @param user     The user name
	 * @param pwd      The user's password
	 */
	public MarkLogicDataMovement(Config config) throws Exception {
		// Set up a new Client
		DigestAuthContext authContext = new DatabaseClientFactory.DigestAuthContext(config.getMLUser(), config.getMLPassword());
		DatabaseClient dbClient = DatabaseClientFactory.newClient(config.getMLHost(), config.getMLStagingDbPort(), authContext);
		this.manager = dbClient.newDataMovementManager();
	}

	/**
	 * Create a WriteBatcher and start a job using it
	 * 
	 * @param jobName
	 * @return the WriteBatcher object
	 */
	public WriteBatcher startJob(String jobName) {
		final WriteBatcher writer = manager.newWriteBatcher();

		// Set up the job properties
		writer.withJobName(jobName);
		writer.withBatchSize(50);
		writer.onBatchSuccess(batch -> {
			System.out.append("Batch success");
		});
		writer.onBatchFailure((batch, throwable) -> throwable.printStackTrace());

		// Start the job (also assigns a ticket)
		manager.startJob(writer);
		return writer;
	}

	/**
	 * Add a Data Hub Document
	 * 
	 * @param writer     An initialized WriteBatcher
	 * @param uri        The document's URI in MarkLogic
	 * @param datastream An input stream of document content (bytes, file, etc)
	 */
	public void addDocument(WriteBatcher writer, String uri, InputStream datastream) {
		// Set expected metadata for Data Hub document
		DocumentMetadataHandle dmdh = new DocumentMetadataHandle();
		dmdh.withMetadataValue("datahubCreatedBy", "S3-Input");
		dmdh.withMetadataValue("datahubCreatedByStep", "");
		dmdh.withMetadataValue("datahubCreatedInFlow", "");
		dmdh.withMetadataValue("datahubCreatedOn", Instant.now().toString());
		dmdh.withMetadataValue("datahubCreatedByJob", writer.getJobId());
		dmdh.withCollections("Ingestion");

		// InputStreamHandle streamHandle = new InputStreamHandle(datastream);
		writer.addAs(uri, dmdh, datastream);
	}

	/**
	 * Complete the job
	 * 
	 * @param writer An active WriteBatcher
	 */
	public void finishJob(WriteBatcher writer) {
		writer.flushAndWait();
		manager.stopJob(writer.getJobTicket());
	}
}
