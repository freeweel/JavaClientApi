package main;

import java.io.OutputStream;
import java.util.List;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;

public class AmazonS3Util {
	// Access Key and Secret Keys from AWS for a IAM Role with at least S3ReadOnly permissions
    private final String ACCESS_KEY="AKIA2OQY4SPUVUIG325S";
    private final String SECRET_KEY="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";
	
	private AmazonS3 s3;

	/**
	 * Constructor (initializes session with credentials)
	 */
    public AmazonS3Util() throws Exception {
		BasicAWSCredentials awsCreds = new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY);
		this.s3 = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(awsCreds)).withRegion(Regions.US_EAST_2).build();
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
	 * Get an S3 Document's content
	 * @param bucketName
	 * @param s3FileKey
	 * @throws Exception
	 */
	public void readDocToStream(OutputStream out, String bucketName, String s3FileKey) throws Exception {
		GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, s3FileKey);
		S3Object s3Object = s3.getObject(getObjectRequest);
		s3Object.getObjectContent().transferTo(out);
	}

}
