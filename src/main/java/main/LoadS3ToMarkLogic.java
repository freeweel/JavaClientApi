package main;

import aws.AmazonS3Util;
import common.Config;
import marklogic.MarkLogicDataMovementUtil;
import marklogic.MarkLogicStreamWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example Main Module that shows how to load S3 Data into MarkLogic
 */
public final class LoadS3ToMarkLogic {
    private static Logger LOGGER = LoggerFactory.getLogger("Log");

    /**
     * Load S3 Data into MarkLogic
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int total = 0;
        try {
            // Create config object from config file
            Config config = Config.getConfig();

            // Load all S3 files with this bucket and object prefix (e.g., can work like a directory path)
            String bucketName = config.AWS_BUCKET;

            // Create MarkLogic data movement instance and start the load job
            MarkLogicDataMovementUtil dmsdk = new MarkLogicDataMovementUtil();
            MarkLogicStreamWriter markLogicStreamWriter = new MarkLogicStreamWriter(dmsdk);
            dmsdk.startWriteJob("S3-Write");

            // Instantiate an S3 Utility and find and load the docs from S3
            AmazonS3Util s3Util = new AmazonS3Util();
            s3Util.loadDocs(markLogicStreamWriter);

            // Flush jobs and close write batch
            dmsdk.closeWriteJob();
        } catch (Exception e) {
            LOGGER.error("Error occurred while processing upload " + e.getMessage());
        } finally {
            LOGGER.info("Successfully loaded " + total + " documents");
        }
    }
}
