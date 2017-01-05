
package com.pivotal.samples.AWSServiceBroker.Producer;


import java.awt.image.BufferedImage;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import javax.imageio.ImageIO;

import org.json.JSONObject;

import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class App {
	static int TIMEOUT = 10000;
	static String [] allowedImageExtensions = {"jpg", "jpeg", "png", "gif"};
	
	

	static final String AWS_CREDENTIALS_ENV_VAR = "credentials";
	static final String AWS_ACCESS_KEY_ENV_VAR = "access_key_id";
	static final String AWS_SECRET_KEY_ENV_VAR = "secret_access_key";
	static final String AWS_BUCKET_ENV_VAR = "bucket";
	static final String AWS_QUEUE_ENV_VAR = "queue_name";
	static final String VCAP_SERVICES_ENV_VAR = "VCAP_SERVICES";
	static final String AWS_S3_ENV_VAR = "s3";
	static final String AWS_SQS_ENV_VAR = "sqs";
	static final String AWS_REGION_ENV_VAR = "region";

	
	static final String MONITORED_DIRECTORY_ENV_VAR = "MONITORED_DIRECTORY";
	//These two need to be coordinated
	static final String IMAGE_FORMAT = "PNG";
	static final String IMAGE_CONTENT_MIME_TYPE = "image/png";
	
	 public static void main(String[] args) throws IOException {

		String key = null; 
		BufferedImage image = null;
		byte[] imageByteArray = null;
		InputStream stream = null;
		ObjectMetadata metadata = new ObjectMetadata();
		String queueURL = null;
		File[] directoryListing = null;
		File dir = null;
		
		 System.out.println("Getting credentials from environment...");
    	//Get credentials from the environment
    	Map<String, String> envVar = System.getenv();    	    	
        JSONObject obj = new JSONObject(envVar.get(VCAP_SERVICES_ENV_VAR));
        
        JSONObject s3Obj = obj.getJSONArray(AWS_S3_ENV_VAR).getJSONObject(0).getJSONObject(AWS_CREDENTIALS_ENV_VAR);
        String bucketName = s3Obj.getString(AWS_BUCKET_ENV_VAR);
    	String bucketAccessKey = s3Obj.getString(AWS_ACCESS_KEY_ENV_VAR);
    	String bucketSecretKey = s3Obj.getString(AWS_SECRET_KEY_ENV_VAR);
    	String bucketRegion = s3Obj.getString(AWS_REGION_ENV_VAR);
    	
    	JSONObject sqsObj = obj.getJSONArray(AWS_SQS_ENV_VAR).getJSONObject(0).getJSONObject(AWS_CREDENTIALS_ENV_VAR);
    	String queueName = sqsObj.getString(AWS_QUEUE_ENV_VAR);
    	String queueAccessKey = sqsObj.getString(AWS_ACCESS_KEY_ENV_VAR);
    	String queueSecretKey = sqsObj.getString(AWS_SECRET_KEY_ENV_VAR);
    	String queueRegion = sqsObj.getString(AWS_REGION_ENV_VAR);
    	
    	System.out.println("Initializing AWS Client...");
    	//Set up credentials and AWS clients
    	BasicAWSCredentials myS3Creds = new BasicAWSCredentials(bucketAccessKey, bucketSecretKey);
    	AmazonS3 s3 = new AmazonS3Client(myS3Creds);
    	s3.setRegion(Region.getRegion(Regions.fromName(bucketRegion)));

    	BasicAWSCredentials mySQSCreds = new BasicAWSCredentials(queueAccessKey, queueSecretKey);
        AmazonSQS sqs = new AmazonSQSClient(mySQSCreds);
        sqs.setRegion(Region.getRegion(Regions.fromName(queueRegion)));
	    
        String monitoredDirectory = envVar.get(MONITORED_DIRECTORY_ENV_VAR);	
		
	    //Content type stays constant
	    metadata.setContentType(IMAGE_CONTENT_MIME_TYPE);
	    	
	    //Poll local directory for changes - Loop infinitely
	    System.out.println("Polling Directory " + monitoredDirectory);
        while (true) {
	    	 try {
	    		 Thread.sleep(TIMEOUT);
		    	 dir = new File(monitoredDirectory);
		    	 assert(dir.isDirectory());
		    	  directoryListing = dir.listFiles();
		    	  if (directoryListing != null) {
		    	    for (File fileToUpload : directoryListing) {
		    	    	if (fileToUpload.isFile() && isImage(fileToUpload)) {
		    	    		
		    	    		//Read the file
		    	    		image = ImageIO.read(fileToUpload);
				            
		    	    		System.out.println("Uploading file: " + fileToUpload.getName());
				            //Upload the file with metadata
				            imageByteArray = getImageData(image);
			                stream = new ByteArrayInputStream(imageByteArray);
			                metadata.setContentLength(imageByteArray.length);
			                key = UUID.randomUUID().toString();
			                s3.putObject(new PutObjectRequest(bucketName, key, stream, metadata));
			                fileToUpload.delete();
			                System.out.println("Object key: " + key);
				            
			                System.out.println("Updating Queue...");
				            // Send a message on the queue
				            sqs.sendMessage(new SendMessageRequest(queueURL, key));
		    	    	}
		    	    }
		    	   }
		        } catch (AmazonServiceException ase) {
		            System.out.println("Error Message:    " + ase.getMessage());
		            
		        } catch (AmazonClientException ace) {
		            System.out.println("Error Message: " + ace.getMessage());
		        } 
	    	 catch (InterruptedException e) {
					e.printStackTrace();
				}
	     }

	       
	    }
	 
	 private static byte[] getImageData(BufferedImage image) throws IOException {
			
			ByteArrayOutputStream outputStream;
			byte[] imageByteArray;
			
			outputStream = new ByteArrayOutputStream();
			ImageIO.write(image, IMAGE_FORMAT, outputStream);
			outputStream.flush();
			imageByteArray = outputStream.toByteArray();
			outputStream.close();
			
			return imageByteArray;
	}

	static boolean isImage (File file) {
		  //Simple test to see if the file is an image file based on extension (even though that is no guarantee). 
		  boolean retVal = false;
		  for (String ext : allowedImageExtensions) {
			  if (file.getName().endsWith(ext)) {
				  retVal = true;
				  break;
			  }
		  }
		  return retVal;
	  }
    static void printEnv(Map<String, String> env) {
    	Set<Entry<String, String>> values = env.entrySet();
    	Iterator<Entry<String, String>> i = values.iterator();
    	Entry <String, String> e;
    	System.out.println("--------------------------ENV VALUE--------------------------");
    	while (i.hasNext()) {
    		e = i.next();    		
    		System.out.println("KEY NAME: " + e.getKey());
    		System.out.println("KEY VALUE: " + e.getValue());
    	}
    	System.out.println("--------------------------ENV VALUE--------------------------");
    }


}
