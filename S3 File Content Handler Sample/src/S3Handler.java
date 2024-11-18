package com.accusoft.pdjs3;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.SetBucketVersioningConfigurationRequest;

/**
 * The S3Handler class provides utility methods for accessing documents in Amazon S3 buckets
 * and performing operations such as getting, uploading, listing, and deleting documents.
 */
public class S3Handler {

    private String accessKeyId;
    private String secretAccessKey;
    private String region;
    private String bucketName;
    private String folderName = "";
    private AmazonS3 s3Client;

    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(S3Handler.class);

    /**
     * Constructs an S3Handler object with the specified credentials and region.
     *
     * @param accessKeyId the access key ID for the Amazon S3 client
     * @param secretAccessKey the secret access key for the Amazon S3 client
     * @param region the region for the Amazon S3 client
     */
    public S3Handler(String accessKeyId, String secretAccessKey, String region) {
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
        this.region = region;
    }
    
    /**
     * Creates a new AmazonS3 client with the configured credentials and region.
     *
     * @param accessKeyId the access key ID for the Amazon S3 client
     * @param secretAccessKey the secret access key for the Amazon S3 client
     * @param region the region for the Amazon S3 client
     *
     * @throws AmazonS3Exception if an error occurs while creating the AmazonS3 client
     * 
     * @return a new AmazonS3 client object
     */
    private AmazonS3 createAmazonS3Client(String accessKeyId, String secretAccessKey, String region) throws AmazonS3Exception {
        validateAwsCredentials(accessKeyId, secretAccessKey, region);
    
        try {
            BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);
            return AmazonS3ClientBuilder.standard()
                    .withRegion(region)
                    .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                    .build();
        } catch (AmazonS3Exception e) {
            logger.error("Error creating Amazon S3 client", e);
            throw e;
        }
    }
    
    /**
     * Validates the AWS credentials and region.
     *
     * @param accessKeyId the access key ID for the Amazon S3 client
     * @param secretAccessKey the secret access key for the Amazon S3 client
     * @param region the region for the Amazon S3 client
     *
     * @throws AmazonS3Exception if the access key ID, secret access key, or region is empty
     */
    private void validateAwsCredentials(String accessKeyId, String secretAccessKey, String region) throws AmazonS3Exception {
        if (accessKeyId.isEmpty()) {
            logger.info("AWS Access key ID is required");
            throw new AmazonS3Exception("Access key ID is required");
        }
    
        if (secretAccessKey.isEmpty()) {
            logger.info("AWS Secret access key is required");
            throw new AmazonS3Exception("Secret access key is required");
        }
    
        if (region.isEmpty()) {
            logger.info("S3 Region is required");
            throw new AmazonS3Exception("Region is required");
        }
    }

    /**
     * Validates the bucket name.
     *
     * @param bucketName the name of the bucket to validate
     *
     * @throws AmazonS3Exception if the bucket name is empty
     */
    private void validateBucketName(String bucketName) throws AmazonS3Exception {
        if (bucketName.isEmpty()) {
            logger.info("S3 Bucket name is required");
            throw new AmazonS3Exception("Bucket name is required");
        }
    }

    /**
     * Validates the document name.
     *
     * @param documentName the name of the document to validate
     *
     * @throws AmazonS3Exception if the document name is empty
     */
    private void validateDocumentName(String documentName) throws AmazonS3Exception {
        if (documentName.isEmpty()) {
            logger.info("Document name is required");
            throw new AmazonS3Exception("Document name is required");
        } 
    }

    /**
     * Validates the folder name and removes trailing slashes if present.
     *
     * @param folderName the name of the folder to validate
     *
     */ 
    private void validateFolderName(String folderName) {
        if (folderName.endsWith("/") && folderName.length() > 1) {
            folderName = folderName.substring(0, folderName.length() - 1);
        }
    }

    /**
     * Generate document key for the specified folder and document name.
     *
     * @param folderName the name of the folder containing the S3 object, or null if the object is in the root of the bucket
     * @param documentName the name of the S3 object to retrieve
     *
     * @return the document key as a String. 
     */
    private String getDocumentKey(String folderName, String documentName) {
        if(folderName != null){
            validateFolderName(folderName);
        }
        StringBuilder keyBuilder = new StringBuilder();
        if (folderName != null && !folderName.isEmpty()) {
            keyBuilder.append(folderName).append("/");
        }
        keyBuilder.append(documentName);
        return keyBuilder.toString();
    }

    /**
     * Retrieves an S3 object from the specified Amazon S3 bucket and folder.
     *
     * @param bucketName the name of the bucket containing the S3 object
     * @param folderName the name of the folder containing the S3 object
     * @param documentName the name of the S3 object to retrieve
     * 
     * @throws AmazonS3Exception if an error occurs while retrieving the S3 object
     *
     * @return the S3 object with the specified name
     */
    private S3Object getS3Object(String bucketName, String folderName, String documentName) throws AmazonS3Exception {
        validateBucketName(bucketName);
        validateDocumentName(documentName);

        if (s3Client == null) {
            s3Client = createAmazonS3Client(accessKeyId, secretAccessKey, region);
        }

        if (documentName.isEmpty()) {
            logger.info("Document name is required");
            return null;
        }

        String key = getDocumentKey(folderName, documentName);

        logger.info("Retrieving {} S3 ", key);

        try {
            return s3Client.getObject(new GetObjectRequest(bucketName, key));
        } catch (AmazonS3Exception e) {
            logger.error("Error retrieving {} from S3: {}", key, e.getMessage());
            return null;
        }
    }

    /**
     * Checks if the specified file exists in the specified Amazon S3 bucket and folder.
     *
     * @param bucketName the name of the bucket containing the file
     * @param folderName the name of the folder containing the file
     * @param documentName the name of the document to check for
     *
     * @return true if the file exists, false otherwise
     */
    public boolean doesS3FileExist(String bucketName, String folderName, String documentName) throws AmazonS3Exception {
        validateBucketName(bucketName);
        validateDocumentName(documentName);
        String key;
        if (s3Client == null) {
            s3Client = createAmazonS3Client(accessKeyId, secretAccessKey, region);
        }
        
   
        documentName = getDocumentKey(folderName, documentName);
    
        
        try {
            return s3Client.doesObjectExist(bucketName, documentName);
        } catch (AmazonS3Exception e) {
            e.printStackTrace();
            return false;
        }
    }



    /**
     * Retrieves a DataInputStream containing the contents of the specified document in Amazon S3.
     *
     * @param documentName the name of the document to retrieve
     * @param bucketName the name of the bucket containing the document
     * @param folderName the name of the folder containing the document
     * @return a DataInputStream containing the contents of the document
     *
     * @throws IOException if an I/O error occurs while reading the document from Amazon S3
     * @throws AmazonS3Exception if an error occurs while retrieving the document from Amazon S3
     */
    public DataInputStream getFileS3DataInputStream(String documentName, String bucketName, String folderName) throws IOException {
        validateBucketName(bucketName); 
        validateDocumentName(documentName);

        if (s3Client == null) {
            s3Client = createAmazonS3Client(accessKeyId, secretAccessKey, region);
        }

        S3Object s3Object = null;

        if (!doesS3FileExist(bucketName, folderName, documentName)) {
            logger.info("{} could not be found in S3 bucket", documentName);
            return null;
        } else {
            try {
                s3Object = getS3Object(bucketName, folderName, documentName);
                if (s3Object != null) {
                    InputStream input = s3Object.getObjectContent().getDelegateStream();
                    DataInputStream dis = new DataInputStream(input);
                    return dis;
                }
            } catch (AmazonS3Exception e) {
                logger.error("Error retrieving S3 object: {}", e.getMessage());
                return null;
            }
        }
        return null;
    }

    /**
     * Retrieves a byte array containing the contents of the specified document in Amazon S3.
     *
     * @param documentName the name of the document to retrieve
     * @param bucketName the name of the bucket containing the document
     * @param folderName the name of the folder containing the document
     * @return a byte array containing the contents of the document
     *
     * @throws IOException if an I/O error occurs while reading the document from Amazon S3
     * @throws AmazonS3Exception if an error occurs while retrieving the document from Amazon S3
     */
    public byte[] getFileS3Bytes(String documentName, String bucketName, String folderName) throws IOException, AmazonS3Exception {
        validateBucketName(bucketName); 
        validateDocumentName(documentName);

        if (s3Client == null) {
            s3Client = createAmazonS3Client(accessKeyId, secretAccessKey, region);
        }

        S3Object s3Object = null;

        if (!doesS3FileExist(bucketName, folderName, documentName)) {
            logger.info("{} could not be found in S3 bucket", documentName);
            return null;
        } else {
            try {
                s3Object = getS3Object(bucketName, folderName, documentName);
                if (s3Object != null) {
                    try (InputStream input = s3Object.getObjectContent()) {
                        ByteArrayOutputStream output = new ByteArrayOutputStream();

                        int len;
                        byte[] buffer = new byte[4096];

                        while ((len = input.read(buffer, 0, 4096)) != -1) {
                            output.write(buffer, 0, len);
                        }

                        if (output.size() == 0) {
                            logger.info("Document is empty");
                            return null;
                        } else {
                            return output.toByteArray();
                        }

                    } catch (IOException e) {
                        logger.error("Error retrieving document from S3: {}", e.getMessage());
                        return null;
                    }
                }
            } catch (AmazonS3Exception e) {
                logger.error("Error retrieving S3 object: {}", e.getMessage());
                return null;
            } finally {
                if (s3Object != null) {
                    s3Object.close();
                }
            }
        }
        return null;
    }

    /**
     * Deletes a file from the specified Amazon S3 bucket and folder.
     *
     * @param documentName    the name of the file to delete
     * @param bucketName  the name of the bucket containing the file
     * @param folderName  the name of the folder containing the file (optional)
     */
    public void deleteFileFromS3(String documentName, String bucketName, String folderName) throws AmazonS3Exception, AmazonServiceException {

        validateBucketName(bucketName);
        validateDocumentName(documentName);

        if (s3Client == null) {
            s3Client = createAmazonS3Client(accessKeyId, secretAccessKey, region);
        }

        String key = getDocumentKey(folderName, documentName);

        try {
            // Delete the file from the S3 bucket
            s3Client.deleteObject(bucketName, key);
            logger.info("deleting {} from S3", key);

        } catch (AmazonServiceException e) {
            e.printStackTrace();
        } catch (AmazonClientException e) {
            e.printStackTrace();
        }
    }

    /**
     * Saves a file to the specified Amazon S3 bucket and folder with auto-versioning.
     *
     * @param documentName    the name of the file to upload
     * @param file        the file to upload
     * @param bucketName  the name of the bucket to upload the file to
     * @param folderName  the name of the folder to upload the file to (optional) 
     *
     * @throws AmazonS3Exception if an error occurs while saving the file to Amazon S3
     */
    public void saveFileToS3(String documentName, File file, String bucketName, String folderName) throws AmazonS3Exception {

        validateBucketName(bucketName);
        validateDocumentName(documentName);
        
        if (file == null) {
            logger.info("File to upload is required");
            throw new AmazonS3Exception("File is required");
        }

        String key = getDocumentKey(folderName, documentName);

        if (s3Client == null) {
            s3Client = createAmazonS3Client(accessKeyId, secretAccessKey, region);
        }

        try {
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(file.length());

            PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, key, file)
                    .withMetadata(metadata);

            // Enable auto-versioning (enabled by default)
            s3Client.setBucketVersioningConfiguration(
                    new SetBucketVersioningConfigurationRequest(
                            bucketName,
                            new BucketVersioningConfiguration(BucketVersioningConfiguration.ENABLED)));

            s3Client.putObject(putObjectRequest);
            logger.info("{} saved to S3", documentName);

        } catch (AmazonS3Exception e) {
            logger.error("Error saving {} to S3: {}", documentName, e.getMessage());
            throw e;
        }
    }
    
    /**
     * Lists the objects in the specified Amazon S3 bucket and folder.
     *
     * @param bucketName the name of the bucket to list objects from
     * @param folderName the name of the folder to list objects from (optional)
     *
     * @return an array of object names in the specified bucket and folder
     */
    public String[] listS3BucketObjects(String bucketName, String folderName) throws AmazonS3Exception {

        validateBucketName(bucketName);

        if (s3Client == null) {
            s3Client = createAmazonS3Client(accessKeyId, secretAccessKey, region);
        }
    
        ListObjectsRequest request = new ListObjectsRequest().withBucketName(bucketName).withPrefix(folderName);
        ObjectListing objectListing;
        List<String> filenames = new ArrayList<>();
        int prefixLength = folderName == null || folderName.isEmpty() ? 0 : folderName.length() + 1;

        do {
            objectListing = s3Client.listObjects(request);
            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                if (!objectSummary.getKey().endsWith("/") && !objectSummary.getKey().substring(prefixLength).contains("/") && objectSummary.getKey().length() > prefixLength) {
                    filenames.add(objectSummary.getKey().substring(prefixLength));
                }
            }
            request.setMarker(objectListing.getNextMarker());
        } while (objectListing.isTruncated());
    
        return filenames.toArray(new String[0]);
    }

    /**
     * Retrieves the AWS access key ID.
     *
     * @return the access key ID as a String.
     */
    public String getAccessKeyId() {
        return accessKeyId;
    }

    /**
     * Sets the AWS access key ID.
     *
     * @param accessKeyId the access key ID to set.
     */
    public void setAccessKeyId(String accessKeyId) {
        this.accessKeyId = accessKeyId;
    }

    /**
     * Retrieves the AWS secret access key.
     *
     * @return the secret access key as a String.
     */
    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    /**
     * Sets the AWS secret access key.
     *
     * @param secretAccessKey the secret access key to set.
     */
    public void setSecretAccessKey(String secretAccessKey) {
        this.secretAccessKey = secretAccessKey;
    }

    /**
     * Retrieves the AWS region.
     *
     * @return the region as a String.
     */
    public String getRegion() {
        return region;
    }

    /**
     * Sets the AWS region.
     *
     * @param region the region to set.
     */
    public void setRegion(String region) {
        this.region = region;
    }

    /**
     * Retrieves the name of the Amazon S3 bucket.
     *
     * @return the bucket name as a String.
     */
    public String getBucketName() {
        return bucketName;
    }

    /**
     * Sets the name of the Amazon S3 bucket.
     *
     * @param bucketName the bucket name to set.
     */
    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    /**
     * Retrieves the name of the folder within the Amazon S3 bucket.
     *
     * @return the folder name as a String.
     */
    public String getFolderName() {
        return folderName;
    }

    /**
     * Sets the name of the folder within the Amazon S3 bucket.
     *
     * @param folderName the folder name to set.
     */
    public void setFolderName(String folderName) {
        this.folderName = folderName;
    }
}
