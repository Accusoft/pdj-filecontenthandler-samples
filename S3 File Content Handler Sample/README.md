# PrizmDoc for Java S3 Content Handler

The `S3Handler` class provides utility methods for interacting with Amazon S3 in the `PDJS3ContentHandler` class.
The `PDJS3ContentHandler` class is a content handler responsible for providing a connection between an S3 bucket and the viewer.

The S3 content handler supports VirtualDocuments, CompoundDocuments and SparseDocuments.

--- 
### VirtualDocument

Where `1.pdf` and `2.pdf` are available as individual documents to select, but can be combined into 1 big virtual document.

```bash
http://localhost:8080/virtualviewer/?documentId=VirtualDocument:1.pdf,2.pdf
```
--- 

### SparseDocument

Where `sparseDoc` is actually a directory with single page images or documents, that will concatenate into one document.


```bash
http://localhost:8080/virtualviewer/?documentId=SparseDocument:sparseDoc
```
--- 

## Building the PDJS3ContentHandler

Set the following as ENV variables


```bash
export AWS_ACCESS_KEY_ID="myAccessKey"
export AWS_SECRET_ACCESS_KEY="mySecretAccessKey"
export S3_REGION_NAME="my-region"
export S3_BUCKET_NAME="myBucket"
export S3_FOLDER_NAME="mySampleFolder"
```

Run the following commands:

```bash
cd s3handler
```

```bash
mvn clean package -f pom.xml
```

Plug-in jar will export to:

```bash
s3handler/target/PDJS3ContentHandler-1.0.jar
```

Javadocs will export to:

```bash
s3handler/target/apidocs
```

## Configuring the PDJS3ContentHandler

To use the PDJS3ContentHandler, add `PDJS3ContentHandler-1.0.jar` to `WEB-INF/lib` and configure the following web.xml parameters:

Change the existing `contentHandlerClass` to `PDJS3ContentHandler`

```xml
    <init-param>
        <param-name>contentHandlerClass</param-name>
        <param-value>com.accusoft.pdjs3.PDJS3ContentHandler</param-value>
    </init-param>
```

Add the following parameters, and fill out values for `AwsAccessKeyId`, `AwsSecretAccessKey`, `s3RegionName`, `s3BucketName`, and **optionally** `s3FolderName`.

```xml
    <init-param>
        <param-name>AwsAccessKeyId</param-name>
        <param-value></param-value>
    </init-param>
    <init-param>
        <param-name>AwsSecretAccessKey</param-name>
        <param-value></param-value>
    </init-param>
    <init-param>
        <param-name>s3RegionName</param-name>
        <param-value></param-value>
    </init-param>
    <init-param>
        <param-name>s3BucketName</param-name>
        <param-value></param-value>
    </init-param>
    <init-param>
        <param-name>s3FolderName</param-name>
        <param-value></param-value>
    </init-param>
```

## S3Handler Usage Examples

Create an instance of `S3Handler`:

```java
S3Handler s3Handler = 
    new S3Handler(
        String accessKeyId, 
        String secretAccessKey, 
        String region); 
```

### Listing all objects in an S3 bucket/folder

```java
String[] array = s3Handler.listS3BucketObjects(String s3BucketName, String s3FolderName);
```

### Retrieve document/resource using byte[]

```java
byte[] bytes = s3Handler.getFileS3Bytes(String documentName, String bucketName, String folderName);
```

### Retrieve document using DataInputStream

```java
DataInputStream dis = getFileS3DataInputStream(String documentName, String bucketName, String folderName)
```

### Save a file to S3 Bucket and optionally a folder

```java
s3Handler.saveFileToS3(String documentName, File fileToSave, String s3BucketName, String s3FolderName);
```

### Delete a file from an S3 Bucket and optionally a folder

```java
s3Handler.deleteFileFromS3(String documentName, String s3BucketName, String s3FolderName);
```


