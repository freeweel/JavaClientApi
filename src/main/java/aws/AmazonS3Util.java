package aws;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import common.Config;
import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * Utility functions for downloading and uploading S3 documents.
 * This class gets information from the common.Config in order to
 * get Access Key information as well as S3 bucket name.
 *
 */
public class AmazonS3Util {
	private AmazonS3 s3;
	private String bucket;

	/**
	 * Constructor (initializes session with credentials)
	 */
    public AmazonS3Util() throws Exception {
		Config config = Config.getConfig();
		// Get AWS Credentials using values found in AccessKeys.secret
		BasicAWSCredentials awsCreds = new BasicAWSCredentials(config.AWS_KEY, config.AWS_SECRET);
		this.s3 = AmazonS3ClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(awsCreds))
				.withRegion(config.AWS_REGION).build();
		this.bucket = config.AWS_BUCKET;
	}

	/**
	 * Get all the buckets based in the region with the proper access key
	 */
	public List<Bucket> getBuckets() throws Exception {
		return s3.listBuckets();
	}

	/**
	 * Get objects (files) that correspond to the bucket and prefix
	 * @param bucketName The name of the S3 bucket that can be accessed with the access key 
	 * @param prefix The prefix string (e.g., for a directory use the form 'dirname/' or 'dirname/subdirname/');
	 * @return A com.amazonaws.services.s3.model.ListObjectsV2Result object
	 * @throws Exception
	 */
	public ListObjectsV2Result getDocList(String bucketName, String prefix) throws Exception {
		return s3.listObjectsV2(bucketName, prefix); 
	}

	/**
	 * Populate an OutputStream with an S3 Document's content from config.AWS_BUCKET
	 * @param out An OutputStream such as a ByteArrayOutputStream or FileOutputStream
	 * @param s3FileKey The S3 object's full path
	 * @throws Exception
	 */
	public void readDocToStream(OutputStream out, String s3FileKey) throws Exception {
		GetObjectRequest getObjectRequest = new GetObjectRequest(bucket, s3FileKey);
		S3Object s3Object = s3.getObject(getObjectRequest);
		IOUtils.copy(s3Object.getObjectContent(), out);
	}

	/**
	 * Write an live output stream to S3 object in config.AWS_BUCKET
	 * @param s3FileKey The S3 key (full path to object)
	 * @param inputStream The stream that was read to by the source is also used for writing to S3
	 *                    (for some reason S3 uses InputStream to write)
	 * @throws Exception
	 *
	 * NOTE: We are not sending any object metadata at this time
	 */
	public void writeInputStreamToDoc(String s3FileKey, InputStream inputStream) throws Exception {
		ObjectMetadata metadata = new ObjectMetadata();
		PutObjectResult result = s3.putObject(bucket, s3FileKey, inputStream, metadata);
	}
}
