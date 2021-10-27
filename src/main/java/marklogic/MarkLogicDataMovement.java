package marklogic;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.DigestAuthContext;
import com.marklogic.client.datamovement.*;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.StringQueryDefinition;
import common.Config;
import common.StreamWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.time.Instant;

/**
 * This class provides a layer around the Data Movement SDK's
 * DataMovementManager and WriteBatcher
 */
public class MarkLogicDataMovement {
    private DataMovementManager dmManager;
    private DatabaseClient dbClient;
    private static Logger LOGGER = LoggerFactory.getLogger("Log");

    /**
     * Constructor that sets the configuration and
     * creates a new data movement manager
     */
    public MarkLogicDataMovement() throws Exception {
        Config config = Config.getConfig();
        // Set up a new Client
        DigestAuthContext authContext = new DatabaseClientFactory.DigestAuthContext(config.ML_USER, config.ML_PASSWORD);
        this.dbClient = DatabaseClientFactory.newClient(config.ML_HOST, config.ML_STAGING_PORT, authContext);
        this.dmManager = dbClient.newDataMovementManager();
    }

    /**
     * Set up Query batcher that gets content out of MarkLogic and writes to an external system
     * @param outputStreamWriter An output stream writer for Files, S3, or another MarkLogic DB
     * @param collection One or more MarkLogic collections names that are used to find extract data
     * @apiNote This is pure stream writing, so the full object is never in memory.
     * @throws Exception
     */
    public void extractFromMarkLogic(StreamWriter outputStreamWriter, String... collection) throws Exception {
        // Construct a Collection query with which to drive the job.
        QueryManager queryManager = this.dbClient.newQueryManager();
        StringQueryDefinition queryDef = queryManager.newStringDefinition();
        queryDef.setCollections(collection);

        // Set up the batcher
        QueryBatcher batcher = dmManager.newQueryBatcher(queryDef);
        batcher.onUrisReady(
                new ExportListener()
                        .withConsistentSnapshot()
                        .onDocumentReady(doc -> {
                            InputStream inputStream = null;
                            try {
                                // Read input stream from MarkLogic document
                                InputStreamHandle inputStreamHandle = new InputStreamHandle();
                                doc.getContent(inputStreamHandle);
                                inputStream = inputStreamHandle.get();

                                // Build output stream based on type of StreamWriter class used
                                // Could be S3, File, MarkLogic, etc
                                outputStreamWriter.write("/temp/out" + doc.getUri(), inputStream);

                                LOGGER.info("Writing " + doc.getUri());
                            }
                            catch(Exception ex) {
                                LOGGER.error("Unable to write file " + ex.getMessage());
                            }
                        })
        )
                .onQueryFailure(exception -> LOGGER.error("Error running batch!! " + exception.getStackTrace().toString()));

        // Run the Job
        JobTicket ticket = this.dmManager.startJob(batcher);
        batcher.awaitCompletion();
        this.dmManager.stopJob(ticket);
    }

    /**
     * Create a WriteBatcher and start a data movement write job using it
     *
     * @param jobName A unique job name
     * @return the WriteBatcher object
     */
    public WriteBatcher startWriteJob(String jobName) {
        final WriteBatcher writer = dmManager.newWriteBatcher();

        // Set up the job properties
        writer.withJobName(jobName);
        writer.withBatchSize(50);
        writer.onBatchSuccess(batch -> {
            LOGGER.info("Batch run successful");
        });
        writer.onBatchFailure((batch, throwable) -> throwable.printStackTrace());

        // Start the job (also assigns a ticket)
        dmManager.startJob(writer);
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
        dmdh.withMetadataValue("datahubCreatedBy", "S3-Ingest");
        dmdh.withMetadataValue("datahubCreatedByStep", "");
        dmdh.withMetadataValue("datahubCreatedInFlow", "");
        dmdh.withMetadataValue("datahubCreatedOn", Instant.now().toString());
        dmdh.withMetadataValue("datahubCreatedByJob", writer.getJobId());
        dmdh.withCollections("Ingestion");

        writer.addAs(uri, dmdh, datastream);
    }

    /**
     * Complete the job
     * Flushes any unwritten saves from the cache and closes the job
     *
     * @param writer An active WriteBatcher
     */
    public void closeWriteJob(WriteBatcher writer) {
        writer.flushAndWait();
        dmManager.stopJob(writer.getJobTicket());
    }
}
