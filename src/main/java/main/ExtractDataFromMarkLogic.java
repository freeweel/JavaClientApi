package main;

import aws.S3StreamWriter;
import common.FileStreamWriter;
import marklogic.MarkLogicDataMovementUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extract data from MarkLogic and write to other systems such as the file system
 * or an AWS S3 Bucket.
 */
public class ExtractDataFromMarkLogic {
    @SuppressWarnings("unused")
    private static final Logger LOGGER = LoggerFactory.getLogger("Log");

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        String[] collection = {"Person-Ingest"};
        MarkLogicDataMovementUtil dmsdk = new MarkLogicDataMovementUtil();
        // Write to S3
        dmsdk.extractFromMarkLogic(new S3StreamWriter(), collection);
        // Write to File System
        dmsdk.extractFromMarkLogic(new FileStreamWriter(), collection);
    }
}
