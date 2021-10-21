package marklogic;

import java.io.InputStream;
import java.time.Instant;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.DigestAuthContext;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.io.DocumentMetadataHandle;
import config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides a layer around the Data Movement SDK's
 * DataMovementManager and WriteBatcher
 */
public class MarkLogicDataMovement {
	private DataMovementManager manager;
	private static Logger LOGGER = LoggerFactory.getLogger("Log");

	/**
	 * Constructor that sets the configuration and
	 * creates a new data movement manager
	 * 
	 * @param config An initialized Config object
	 */
	public MarkLogicDataMovement(Config config) {
		// Set up a new Client
		DigestAuthContext authContext = new DatabaseClientFactory.DigestAuthContext(config.ML_USER, config.ML_PASSWORD);
		DatabaseClient dbClient = DatabaseClientFactory.newClient(config.ML_HOST, config.ML_STAGING_PORT, authContext);
		this.manager = dbClient.newDataMovementManager();
	}

	/**
	 * Create a WriteBatcher and start a data movement job using it
	 * 
	 * @param jobName A unique job name
	 * @return the WriteBatcher object
	 */
	public WriteBatcher startJob(String jobName) {
		final WriteBatcher writer = manager.newWriteBatcher();

		// Set up the job properties
		writer.withJobName(jobName);
		writer.withBatchSize(50);
		writer.onBatchSuccess(batch -> {
			LOGGER.info("Batch run successful");
		});
		writer.onBatchFailure((batch, throwable) -> throwable.printStackTrace());

		// Start the job (also assigns a ticket)
		manager.startJob(writer);
		return writer;
	}

	/**
	 * Add a Data Hub Document
	 * This include adding the metadata used by data hub, and it
	 * adds the new document to an "Ingestion" collection.
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
	 * Flushes any unwritten saves from the cache and closes the job
	 * 
	 * @param writer An active WriteBatcher
	 */
	public void finishJob(WriteBatcher writer) {
		writer.flushAndWait();
		manager.stopJob(writer.getJobTicket());
	}
}
