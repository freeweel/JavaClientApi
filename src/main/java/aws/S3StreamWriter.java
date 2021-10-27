package aws;

import common.StreamWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

/**
 * Save the contents read in via an InputStream to an S3 object
 *
 * NOTE: S3 write takes an InputStream for some odd reason, so the InputStream
 *       read from the source can be used directly to populate S3.
 * NOTE: The stream contents are never stored in memory
 */
public class S3StreamWriter implements StreamWriter {
    private AmazonS3Util s3Util;
    private common.Config config = common.Config.getConfig();
    private static final Logger LOGGER = LoggerFactory.getLogger("Log");

    public S3StreamWriter() throws Exception {
        s3Util = new AmazonS3Util();
    }

    @Override
    public void write(String path, InputStream inputStream) throws Exception {
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        try {
            s3Util.writeInputStreamToDoc(path, inputStream);
        }
        catch (Exception e) {
            LOGGER.error("Unable to write file");
        }
        finally {
            inputStream.close();
        }
    }
}
