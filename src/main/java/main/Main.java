package main;

import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.marklogic.client.datamovement.WriteBatcher;

import org.apache.commons.io.output.ByteArrayOutputStream;

import config.Config;
import config.Config.ENV;
public final class Main {

	public static void main(String[] args) throws Exception{
		Config config = Config.getConfig(ENV.local);
		System.out.println("Test 1 2  3");
		MarkLogicWriter writer = new MarkLogicWriter(config.server(), config.portStaging(), config.user(), config.pwd());
		WriteBatcher writeBatcher = writer.startJob("S3-Write");
		
		// Get list of documents from named S3 bucket within the specified directory
		String bucketName = "marklogic-glue-bucket";
		AmazonS3Util s3Util = new AmazonS3Util();
		ListObjectsV2Result s3Results = s3Util.getDocList(bucketName, "Paul/");
		for (S3ObjectSummary summary : s3Results.getObjectSummaries()) {
			String s3FileKey = summary.getKey();
			long size = summary.getSize();
			System.out.println(s3FileKey + " " + size);
			// String fileName = FilenameUtils.getName(s3FileKey);

			// If document has content then upload to MarkLogic
			if (size > 0) {
				ByteArrayOutputStream output = new ByteArrayOutputStream();
				try {
					s3Util.readDocToStream(output, bucketName, s3FileKey);
					String uri = String.format("/Ingested/%s", s3FileKey);
					writeBatcher.addAs(uri, output.toInputStream());
				}
				finally {
					output.close();
				}
			}
		}

		// Flush jobs and close write batch
		writer.finishJob(writeBatcher);
	}
}
