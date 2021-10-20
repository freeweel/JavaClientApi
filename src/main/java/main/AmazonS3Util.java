package main;

import config.Config;

import java.io.OutputStream;
import java.util.List;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.commons.io.IOUtils;

public class AmazonS3Util {
	private AmazonS3 s3;

	/**
	 * Constructor (initializes session with credentials)
	 */
    public AmazonS3Util(Config config) throws Exception {
		// Get AWS Credentials using values found in AccessKeys.secret
		String accessKey = config.getAwsKey();
		String secretKey = config.getAwsSecret();
		String region = config.getAwsRegion();
		BasicAWSCredentials awsCreds = new BasicAWSCredentials(accessKey, secretKey);
		this.s3 = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(awsCreds)).withRegion(region).build();
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
	 * @param bucketName
	 * @param s3FileKey
	 * @throws Exception
	 */
	public void readDocToStream(OutputStream out, String bucketName, String s3FileKey) throws Exception {
		GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, s3FileKey);
		S3Object s3Object = s3.getObject(getObjectRequest);
		IOUtils.copy(s3Object.getObjectContent(), out);
	}
}
