package com.pivotal.samples.AWSServiceBroker.Consumer;

import java.awt.Color;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.Graphics2D;
import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.json.*;

import javax.imageio.ImageIO;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;


public class  App {
	static int TIMEOUT = 10000;
	
	static final String WATERMARK_STRING = "Pivotal Cloud Foundry";
	static final int INNER_BORDER = 10;
	static final int INNER_BORDER_COLOR = 0xFFD700;  //gold
	static final int OUTER_BORDER = 5;
	static final int OUTER_BORDER_COLOR = Color.BLACK.getRGB();
	static final int TEXT_PADDING = 2;
	
	static final int FONT_SIZE = 16;
	static final Font WATERMARK_FONT = new Font("Arial", Font.BOLD, FONT_SIZE);
	static final Color WATERMARK_COLOR = new Color(0, 135, 116); //teal
	
	static final String IMAGE_FORMAT = "PNG";
	
	static final String AWS_CREDENTIALS_ENV_VAR = "credentials";
	static final String AWS_ACCESS_KEY_ENV_VAR = "access_key_id";
	static final String AWS_SECRET_KEY_ENV_VAR = "secret_access_key";
	static final String AWS_BUCKET_ENV_VAR = "bucket";
	static final String AWS_QUEUE_ENV_VAR = "queue_name";
	static final String VCAP_SERVICES_ENV_VAR = "VCAP_SERVICES";
	static final String AWS_S3_ENV_VAR = "s3";
	static final String AWS_SQS_ENV_VAR = "sqs";
	static final String AWS_REGION_ENV_VAR = "region";
	
	static final String IMAGE_CONTENT_MIME_TYPE = "image/png";
	
    public static void main(String[] args)   {
    	
    	System.out.println("Starting Consumer...");
    	
    	String key = null; 
        InputStream stream = null;
        S3Object object = null;
        String queueURL = null;
        ReceiveMessageRequest receiveMessageRequest = null;
        List<Message> messages = null;
        BufferedImage image = null;
        byte[] imageByteArray = null;	
        ObjectMetadata metadata = null;

        
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
    	 
        System.out.println("Starting loop...");
        //Polling for changes - infinitely
        while (true) {
	        try {
	        	Thread.sleep(TIMEOUT);

	        	System.out.println("Reading Queue...");
	        	//read queue
	        	queueURL = sqs.getQueueUrl(queueName).getQueueUrl();
	            receiveMessageRequest = new ReceiveMessageRequest(queueURL);
	            messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
	        	
	            System.out.println("Processing messages...");
	        	//process messages	    
	           for (Message message : messages) {
	        	   System.out.println("Reading Image...");
	            	//Read the image
	            	key = message.getBody();
	                object = s3.getObject(new GetObjectRequest(bucketName, key));
	                image = ImageIO.read(object.getObjectContent());
	                	                
	                System.out.println("Updating Image...");
	                //Update the image
	                image = drawWaterMark(image);
	                
	                System.out.println("Uploading Image...");
	                //Write the data back
	                imageByteArray = getImageData(image);
	                stream = new ByteArrayInputStream(imageByteArray);
	                metadata = new ObjectMetadata();
	                metadata.setContentLength(imageByteArray.length);
	                metadata.setContentType(IMAGE_CONTENT_MIME_TYPE);
	                s3.putObject(new PutObjectRequest(bucketName, key, stream, metadata));
	                sqs.deleteMessage(new DeleteMessageRequest(queueURL, message.getReceiptHandle()));
	                System.out.println("Object uploaded to URL: " + s3.getUrl(bucketName, key));
	            }
	            System.out.println();            
	            
	        } catch (AmazonServiceException ase) {
	            System.out.println("Error Message:    " + ase.getMessage());
	            
	        } catch (AmazonClientException ace) {
	            System.out.println("Error Message: " + ace.getMessage());
	        } catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
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

    private static BufferedImage drawWaterMark(BufferedImage sourceImage) {

    	int originalImageWidth = sourceImage.getWidth();
        int originalImageHeight = sourceImage.getHeight();
         
        //Determine size of watermark text so we know how big to make the final image
        Graphics2D g2d = (Graphics2D) sourceImage.getGraphics();
        g2d.setFont(WATERMARK_FONT);
        FontMetrics fontMetrics = g2d.getFontMetrics();
        Rectangle2D rect = fontMetrics.getStringBounds(WATERMARK_STRING, g2d);
         
        //Determine amount of borders to add (beyond minimum) so the text can fit
        int leftRightBorder = OUTER_BORDER +  INNER_BORDER + (int)Math.ceil((Integer.max((int) rect.getWidth(), originalImageWidth) - originalImageWidth)/2.0);
        int topBottomBorder = OUTER_BORDER + Integer.max(INNER_BORDER,(int) rect.getHeight()) + 2*TEXT_PADDING;
        int newImageWidth = 2*leftRightBorder +  originalImageWidth;
        int newImageHeight = 2*topBottomBorder + originalImageHeight;
         
        //Create the new image with the border and watermark
        BufferedImage newImage = new BufferedImage(newImageWidth, newImageHeight, sourceImage.getType());
        createBorders (newImage);
        BufferedImage copyDestination = newImage.getSubimage(leftRightBorder, topBottomBorder, originalImageWidth, originalImageHeight);
        sourceImage.copyData(copyDestination.getRaster());
         
        //Write the watermark text
        g2d = (Graphics2D) newImage.getGraphics();
        g2d.setColor(WATERMARK_COLOR);
        g2d.setFont(WATERMARK_FONT);
        fontMetrics = g2d.getFontMetrics();
    	g2d.drawString(WATERMARK_STRING,OUTER_BORDER + INNER_BORDER,newImage.getHeight() - OUTER_BORDER - TEXT_PADDING - fontMetrics.getMaxDescent());	
    	
    	return newImage;
	}
    
    public static void createBorders(BufferedImage image) {
    	
    	int width = image.getWidth();
    	int height = image.getHeight();
    	
    	//Create the outer and inner borders. A bit inefficient since we traverse the whole image, 
    	//but the code is simpler
    	for (int i = 0; i < width; i++) {
    		for (int j = 0; j < height; j++) {
    			if ((i < OUTER_BORDER) || (j<OUTER_BORDER) || ((width - i) < OUTER_BORDER) ||((height-j)< OUTER_BORDER)) {
    				image.setRGB(i, j, OUTER_BORDER_COLOR);
    			}
    			else {
    				image.setRGB(i, j, INNER_BORDER_COLOR);
    			}
    		}
    	}
    }
    
}



