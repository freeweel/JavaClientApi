package aws;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import config.Config;
import org.apache.commons.io.IOUtils;

import java.io.OutputStream;
import java.util.List;

public class AmazonS3Util {
	private AmazonS3 s3;

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
	 * Populate an OutputStream with an S3 Document's content
	 * @param out An OutputStream such as a ByteArrayOutputStream or FileOutputStream
	 * @param bucketName The name of the bucket containing the file
	 * @param s3FileKey The S3 object's full path
	 * @throws Exception
	 */
	public void readDocToStream(OutputStream out, String bucketName, String s3FileKey) throws Exception {
		GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, s3FileKey);
		S3Object s3Object = s3.getObject(getObjectRequest);
		IOUtils.copy(s3Object.getObjectContent(), out);
	}
}
