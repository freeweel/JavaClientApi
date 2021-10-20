package main;

import java.io.OutputStream;
import java.time.Instant;
import java.util.UUID;
import java.util.stream.Stream;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.DigestAuthContext;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.JobTicket;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;

import org.apache.commons.io.IOUtils;

public class MarkLogicWriter {
	private DataMovementManager manager;

	/**
	 * Constructor
	 * 
	 * @param hostname The name or IP address of the host
	 * @param port     The database service port
	 * @param user     The user name
	 * @param pwd      The user's password
	 */
	public MarkLogicWriter(String hostname, int port, String user, String pwd) {
		DigestAuthContext authContext = new DatabaseClientFactory.DigestAuthContext(user, pwd);
		DatabaseClient dbClient = DatabaseClientFactory.newClient(hostname, port, authContext);
		this.manager = dbClient.newDataMovementManager();
	}

	/**
	 * Write some sample content to the database
	 * Used for demo/test
	 * 
	 * @param jobName Some string representing the job
	 * @return The Job ID
	 */
	public String writeBatch(String jobName) {
		final WriteBatcher writer = manager.newWriteBatcher();
		writer.withJobName(jobName);
		writer.withBatchSize(50);
		writer.onBatchSuccess(batch -> {
			System.out.append("well good");
		});
		writer.onBatchFailure((batch, throwable) -> throwable.printStackTrace());

		JobTicket ticket = manager.startJob(writer);

		for (int i = 0; i < 5; i++) {
			final String id = UUID.randomUUID().toString();
			final String now = Instant.now().toString();
			final String status = "active";
			// Call add() as many times as you need, even from multiple threads.
			final String docId = String.format("/PVTEST/%s.json", id);
			final String content = String.format("{\"id\":\"%s\",\"timestamp\":\"%s\",\"status\":\"%s\"}", id, now,
					status);
			writer.add(docId, new DocumentMetadataHandle().withCollections("raw"),
					new StringHandle(content).withFormat(Format.JSON));
		}

		writer.flushAndWait();
		// Finalize the job by its unique handle generated in startJob() above.
		manager.stopJob(ticket);

		return ticket.getJobId();
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

	public void addDocument(WriteBatcher writer, byte[] content, String uri) {
		//Stream<DocumentWriteOperation> op = DocumentWriteOperation.from(null, null);
		//writer.add(op.getUri(), op.getMetadata(), op.getContent());
		//IOUtils.fr
		writer.addAs(uri, content);
	}

	/**
	 * Complete the job
	 * @param writer An active WriteBatcher 
	 */
	public void finishJob(WriteBatcher writer) {
		writer.flushAndWait();
		manager.stopJob(writer.getJobTicket());
	}
}
