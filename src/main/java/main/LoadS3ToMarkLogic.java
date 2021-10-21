package main;

import java.io.File;
import config.Config;
import marklogic.MarkLogicDataMovement;

import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.marklogic.client.datamovement.WriteBatcher;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import aws.AmazonS3Util;

/**
 * Example Main Module that shows how to load S3 Data into MarkLogic
 */
public final class LoadS3ToMarkLogic {
	final private static File configFile = new File("src/main/resources/AccessKeys.secret");
	private static Logger LOGGER = LoggerFactory.getLogger("Log");
	/**
	 * Load S3 Data into MarkLogic
	 * @param args  
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		try {
			// Load all S3 files with this bucket and object prefix (e.g., can work like a directory path)
			String bucketName = "marklogic-glue-bucket";
			String s3ObjectPrefix = "Paul/";

			// Create config object from config file
			Config config = Config.setFile(configFile);

			// Get MarkLogic credentials
			MarkLogicDataMovement writer = new MarkLogicDataMovement(config);
			WriteBatcher writeBatcher = writer.startJob("S3-Write");

			// Get list of documents from named S3 bucket within the specified directory
			AmazonS3Util s3Util = new AmazonS3Util(config);
			ListObjectsV2Result s3Results = s3Util.getDocList(bucketName, s3ObjectPrefix);
			for (S3ObjectSummary summary : s3Results.getObjectSummaries()) {
				String s3FileKey = summary.getKey();
				long size = summary.getSize();
				LOGGER.info(s3FileKey + " " + size);

				// If document has content then upload to MarkLogic with URI: /Ingest/[S3 object key]
				if (size > 0) {
					ByteArrayOutputStream output = new ByteArrayOutputStream();
					try {
						s3Util.readDocToStream(output, bucketName, s3FileKey);
						String uri = String.format("/Ingest/%s", s3FileKey);
						writer.addDocument(writeBatcher, uri, output.toInputStream());
					} finally {
						output.close();
					}
				}
			}

			// Flush jobs and close write batch
			writer.finishJob(writeBatcher);
		}
		catch(Exception e) {
			LOGGER.error("Error occurred while processing upload " + e.getMessage());
		}
	}
}
