package com.accusoft.pdjs3;

import Snow.Format;
import Snow.SnowAnn;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.snowbound.common.transport.AnnotationLayer;
import com.snowbound.common.transport.PermissionLevel;
import com.snowbound.common.transport.VirtualViewerSnowAnn;
import com.snowbound.common.utils.ClientServerIO;
import com.snowbound.common.utils.RasterMaster;
import com.snowbound.common.utils.SnowLoggerFactory;
import com.snowbound.contenthandler.ContentHandlerInput;
import com.snowbound.contenthandler.ContentHandlerResult;
import com.snowbound.contenthandler.VirtualViewerAPIException;
import com.snowbound.contenthandler.VirtualViewerFormatHash;
import com.snowbound.contenthandler.example.FileContentHandler;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.text.StringEscapeUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;

/**
 * This is a sample content handler for demonstration and example purposes. It saves documents and sub-components to the
 * local file system. It should not be used for production instances of VirtualViewer.
 */
public class PDJS3ContentHandler extends FileContentHandler
{
    protected static final String PARAM_BUCKET_NAME = "s3BucketName";
    protected static final String PARAM_FOLDER_NAME = "s3FolderName";
    protected static final String PARAM_REGION_NAME = "s3RegionName";
    protected static final String PARAM_ACCESS_KEY_ID = "AwsAccessKeyId";
    protected static final String PARAM_SECRET_ACCESS_KEY = "AwsSecretAccessKey";

    protected static final String PARAM_TIFF_TAG_ANNOTATIONS = "tiffTagAnnotations";
    protected static final String PARAM_CONTENT_HANDLER_DEBUG = "contentHandlerDebugMode";
    protected static final String PARAM_READ_ONLY_MODE = "fileContentHandlerReadOnlyMode";

    private static final String PREFIX_SPARSE_DOCUMENT = "SparseDocument:";
    private static final String PREFIX_COMPOUND_DOCUMENT = "CompoundDocument:";
    private static final String READ_ONLY_ERROR_MESSAGE = "Saving has been disabled by the administrator";
    private boolean readOnlyMode = false;
    private static boolean gSupportTiffTagAnnotations = false;
    private static boolean contentHandlerDebug = false;

    private static String AwsSecretAccessKey;
    private static String AwsAccessKeyId;
    private static String s3BucketName;
    private static String s3RegionName;
    private static String s3FolderName;
    
    private static final Logger logger = SnowLoggerFactory.getLogger(PDJS3ContentHandler.class);

    @Override
    public void init(ServletConfig config) throws VirtualViewerAPIException {        
        
        AwsSecretAccessKey = config.getInitParameter(PARAM_SECRET_ACCESS_KEY);
        AwsAccessKeyId = config.getInitParameter(PARAM_ACCESS_KEY_ID);
        s3BucketName = config.getInitParameter(PARAM_BUCKET_NAME);
        s3RegionName = config.getInitParameter(PARAM_REGION_NAME);
        s3FolderName = config.getInitParameter(PARAM_FOLDER_NAME);

        validateConfiguration();

        String parseBooleanString = config
                .getInitParameter(PARAM_READ_ONLY_MODE);
        if ("true".equalsIgnoreCase(parseBooleanString)) {
            readOnlyMode = true;
        }

        String tiffTagParam = config
                .getInitParameter(PARAM_TIFF_TAG_ANNOTATIONS);
        if ("true".equalsIgnoreCase(tiffTagParam)) {
            gSupportTiffTagAnnotations = true;
        }

        String debugParam = config.getInitParameter(PARAM_CONTENT_HANDLER_DEBUG);
        if ("true".equalsIgnoreCase(debugParam)) {
            contentHandlerDebug = true;
        }
    }

    public void validateConfiguration() throws VirtualViewerAPIException {

        if(s3BucketName == null && s3BucketName.isEmpty()) {
            logger.error("s3BucketName name not set in web.xml");
            throw new VirtualViewerAPIException("s3BucketName name not set in web.xml");
        }

        if(s3RegionName == null && s3RegionName.isEmpty() ) {
            logger.error("s3RegionName not set in web.xml");
            throw new VirtualViewerAPIException("s3RegionName not set in web.xml");
        }

        if(AwsSecretAccessKey == null && AwsSecretAccessKey.isEmpty()) {
            logger.error("AwsSecretAccessKey not set in web.xml");
            throw new VirtualViewerAPIException("AwsSecretAccessKey not set in web.xml");
        }

        if(AwsAccessKeyId == null && AwsAccessKeyId.isEmpty()) {
            logger.error("AwsAccessKeyId not set in web.xml");
            throw new VirtualViewerAPIException("AwsAccessKeyId not set in web.xml");
        }

        if(s3FolderName != null && !s3FolderName.isEmpty()) {
            logger.info("Initializing PDJS3ContentHandler with bucket: {}, folder: {} region: {}", s3BucketName, s3FolderName, s3RegionName);
        } else {
            logger.info("Initializing PDJS3ContentHandler with bucket: {}, region: {}", s3BucketName, s3RegionName);
        }
    }
    
    /**
     * @throws com.snowbound.contenthandler.VirtualViewerAPIException can throw VirtualViewer exception to raise and log an error
     * @see com.snowbound.contenthandler.interfaces.AvailableDocumentsInterface#getAvailableDocumentIds(ContentHandlerInput)
     */
    @SuppressWarnings("unchecked")
    @Override
    public ContentHandlerResult getAvailableDocumentIds(ContentHandlerInput input)
            throws VirtualViewerAPIException {
        List<String> validExtensions = VirtualViewerFormatHash.getInstance().getKnownExtensions();        
                
        //String clientInstanceId = input.getClientInstanceId();

        S3Handler s3Connector = new S3Handler(AwsAccessKeyId, AwsSecretAccessKey, s3RegionName);
        String[] listArray = s3Connector.listS3BucketObjects(s3BucketName, s3FolderName);

        List<String> validFiles = new ArrayList<>();
        for (String filename : listArray) {
            if (filenameHasKnownExtension(filename, validExtensions)) {
                validFiles.add(filename);
            }
        }
        String[] validDocumentIdArray = new String[validFiles.size()];
        String[] validDisplayNameArray = new String[validFiles.size()];
        for (int validIndex = 0; validIndex < validDocumentIdArray.length; validIndex++) {
            validDocumentIdArray[validIndex] = validFiles.get(validIndex);
        }

        ContentHandlerResult result = new ContentHandlerResult();
        result.put(ContentHandlerResult.KEY_AVAILABLE_DOCUMENT_IDS, validDocumentIdArray);
        result.put(ContentHandlerResult.KEY_AVAILABLE_DISPLAY_NAMES, validDisplayNameArray);
        return result;
    }
    
    /**
     * @throws com.snowbound.contenthandler.VirtualViewerAPIException can throw VirtualViewer exception to raise and log an error
     * @see com.snowbound.contenthandler.interfaces.AnnotationsInterface#getAnnotationNames(ContentHandlerInput)
     */
    @SuppressWarnings("unchecked")
    @Override
    public ContentHandlerResult getAnnotationNames(ContentHandlerInput input)
            throws VirtualViewerAPIException {
        //String clientInstanceId = input.getClientInstanceId();
        String documentKey = scrapeFileNameFromKey(input.getDocumentId());
        List<String> vAnnotationIds = new ArrayList<>();
        String documentFile = documentKey;
        if (gSupportTiffTagAnnotations) {
            try {
                ContentHandlerResult result = getDocumentContent(input);
                byte[] documentContent = result.getDocumentContent();
                if (documentContent != null &&
                    hasTiffTagAnnotations(documentContent)) {
                    vAnnotationIds.add(VirtualViewerSnowAnn.TIFF_TAG_LAYER);
                }
            } catch (VirtualViewerAPIException fsapie) {
                logger.error("Error retrieving TIFF tag annotations", fsapie);
            }
        }

        S3Handler s3Connector = new S3Handler(AwsAccessKeyId, AwsSecretAccessKey, s3RegionName);

        String[] fileList = null;
        try {
            fileList = s3Connector.listS3BucketObjects(s3BucketName, s3FolderName);
        } catch (AmazonS3Exception e) {
            e.printStackTrace();
        }
    
        for (String fileName : fileList) {
            if (!fileName.equals(documentFile) &&
                fileName.indexOf(documentFile) == 0 &&
                fileName.endsWith(".ann")) {
                int nameBegin = documentFile.length() + 1;
                int nameEnd = fileName.lastIndexOf(".ann");
               
                String annotationId = fileName.substring(nameBegin, nameEnd);
                vAnnotationIds.add(annotationId);
            }
        }

        String[] arrayAnnotationIds = new String[vAnnotationIds.size()];
        for (int i = 0; i < arrayAnnotationIds.length; i++) {
            arrayAnnotationIds[i] = (String) vAnnotationIds.get(i);
        }

        ContentHandlerResult result = new ContentHandlerResult();
        result.put(ContentHandlerResult.KEY_ANNOTATION_NAMES, arrayAnnotationIds);
        return result;
    }

    private boolean hasTiffTagAnnotations(byte[] documentContent) {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(documentContent));
        RasterMaster snow = new RasterMaster();
        int filetype = snow.IMGLOW_get_filetype(dis);
        Format format = VirtualViewerFormatHash.getInstance().getFormat(filetype);
        if (!format.isTiff()) {
            return false;
        }
        int pageCount = 0;
        try { 
            snow.IMGLOW_get_pages(dis);
        } catch (InterruptedException e) {
            // If custom code catches a thread interruption, it is best practice to 
            // rethrow the InterruptedException or to re-set the interrupt flag on the thread.
            Thread.currentThread().interrupt();
            return false;
        }
        for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
            int[] value = new int[1];
            byte[] buff = new byte[40000];
            int WANG_ANNOTATION_TAG_ID = 32932;
            int stat = snow.IMGLOW_get_tiff_tag(WANG_ANNOTATION_TAG_ID,
                    buff.length,
                    value,
                    dis,
                    buff,
                    pageIndex);
            if (stat > 0) {
                return true;
            }
        }
        return false;
    }

    private Map<String, String> getExistingAnnotationsHash(String documentId,
            String clientInstanceId)
            throws VirtualViewerAPIException {
        ContentHandlerInput input = new ContentHandlerInput(documentId, clientInstanceId);
        ContentHandlerResult annResult = this.getAnnotationNames(input);
        String[] annNames = annResult.getAnnotationNames();
        Map<String, String> existingAnnHash = new HashMap<>();
        if (annNames != null) {
            for (String existingAnnotationId : annNames) {
                if (existingAnnotationId.contains("-page")) {
                    existingAnnotationId = existingAnnotationId.substring(0, existingAnnotationId.indexOf("-page"));
                }
                existingAnnHash.put(existingAnnotationId, existingAnnotationId);
            }
        }
        return existingAnnHash;
    }

    /**
     * @throws VirtualViewerAPIException can throw VirtualViewer exception to raise and log an error
     * @see com.snowbound.contenthandler.interfaces.AnnotationsInterface#saveAnnotationContent(ContentHandlerInput)
     */
    @Override
    public ContentHandlerResult saveAnnotationContent(ContentHandlerInput input)
            throws VirtualViewerAPIException {
        if (readOnlyMode) {
            throw new VirtualViewerAPIException(READ_ONLY_ERROR_MESSAGE);
        }

        HttpServletRequest request = input.getHttpServletRequest();
        String clientInstanceId = input.getClientInstanceId();
        String documentKey = scrapeFileNameFromKey(input.getDocumentId());
        String annotationKey = input.getAnnotationId();
        int pageSpecificIndex = -1;
        byte[] data = input.getAnnotationContent();
        Map annProperties = input.getAnnotationProperties();
        return saveAnnotationContent(request,
                clientInstanceId,
                documentKey,
                annotationKey,
                pageSpecificIndex,
                data,
                annProperties);
    }

    public ContentHandlerResult saveAnnotationContent(HttpServletRequest request,
            String clientInstanceId,
            String documentKey,
            String annotationKey,
            int pageSpecificIndex,
            byte[] data,
            Map annProperties)
            throws VirtualViewerAPIException {
        if (readOnlyMode) {
            throw new VirtualViewerAPIException(READ_ONLY_ERROR_MESSAGE);
        }

        if (data == null) {
            return null;
        }
        String pageIndexPortion = "";
        /* By default we don't use pageIndex in the filename */
        if (pageSpecificIndex != -1) {
            pageIndexPortion = "-page" + pageSpecificIndex;
        }

        S3Handler s3Connector = new S3Handler(AwsAccessKeyId, AwsSecretAccessKey, s3RegionName);
    
        String baseFilePath = documentKey + "." + annotationKey +
                 pageIndexPortion;
        String fullFilePath = baseFilePath + ".ann";

        if (annProperties != null) {
            //Content handler can retrieve permission level to implement different save behavior
            Boolean redactionFlag = (Boolean) annProperties
                    .get(AnnotationLayer.PROPERTIES_KEY_REDACTION_FLAG);
            Integer permissionLevel = (Integer) annProperties
                    .get(AnnotationLayer.PROPERTIES_KEY_PERMISSION_LEVEL);
        }
        // Make sure any existing ann files are deleted
     
        
        logger.trace("saveAnnotationContent: saving {}", StringEscapeUtils.escapeJava(annotationKey));
        File file = null;

        try {
            file = Files.createTempFile(fullFilePath, null).toFile();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    
        if (data.length > 0) {
            ClientServerIO.saveFileBytes(data, file);
        }
      
        try {
            s3Connector.saveFileToS3(fullFilePath, file, s3BucketName, s3FolderName);
        } catch (AmazonS3Exception e) {
            e.printStackTrace();
        }

        return new ContentHandlerResult();
    }

    /**
     * @throws com.snowbound.contenthandler.VirtualViewerAPIException can throw VirtualViewer exception to raise and log an error
     * @see com.snowbound.contenthandler.interfaces.VirtualViewerContentHandlerInterface#saveDocumentComponents(ContentHandlerInput)
     */
    @Override
    public ContentHandlerResult saveDocumentComponents(ContentHandlerInput input)
            throws VirtualViewerAPIException {
        if (readOnlyMode) {
            throw new VirtualViewerAPIException(READ_ONLY_ERROR_MESSAGE);
        }

        HttpServletRequest request = input.getHttpServletRequest();
        String clientInstanceId = input.getClientInstanceId();
        String documentId = scrapeFileNameFromKey(input.getDocumentId());
        byte[] data = input.getDocumentContent();
        File file = input.getDocumentFile();
        AnnotationLayer[] annotations = input.getAnnotationLayers();
        byte[] bookmarkBytes = input.getBookmarkContent();
        byte[] noteBytes = input.getNotesContent();
        byte[] watermarkBytes = input.getWatermarkContent();
        /* The following line shows how to get the page count if needed. */
        // int pageCount = input.getDocumentPageCount();
        logger.trace("saveDocumentContents");

        if (documentId.startsWith(PREFIX_SPARSE_DOCUMENT)) {
            throw new VirtualViewerAPIException(
                    "Saving documents with the testing prefix \"" + PREFIX_SPARSE_DOCUMENT +
                    "\" is not supported by the sample content handler.");
        } else if (documentId.startsWith(PREFIX_COMPOUND_DOCUMENT)) {
            throw new VirtualViewerAPIException(
                    "Saving documents with the testing prefix \"" + PREFIX_COMPOUND_DOCUMENT +
                    "\" is not supported by the sample content handler.");
        }

        if (data != null) {
            saveDocumentContent(request, clientInstanceId, documentId, data);
        } else if (file != null) {
            saveDocumentContent(request, clientInstanceId, documentId, file);
        }
        
        if (annotations != null) {
            for (AnnotationLayer annLayer : annotations) {
                /*
                * Remove the annLayer from the existingHash to indicate that it
                * should still exist and not be deleted.
                */
                if (annLayer.isNew() || annLayer.isModified()) {
                    saveAnnotationContent(request,
                            clientInstanceId,
                            documentId,
                            annLayer.getLayerName(),
                            annLayer.getPageSpecificIndex(),
                            annLayer.getData(),
                            annLayer.getProperties());
                } else {
                    logger.trace("Skipping unmodified Layer: {}", StringEscapeUtils.escapeJava(annLayer.getLayerName()));
                }
            }
            /* Any annotation that is still in the existing hash should be deleted */
            deleteUnsavedExistingLayers(documentId, input.getDeletedAnnotationLayers());
        }
        
        if(noteBytes != null) {
            saveNotesContent(clientInstanceId, documentId, noteBytes);
        }
        
        if(bookmarkBytes != null) {
            saveBookmarkContent(clientInstanceId, documentId, bookmarkBytes);
        }
        
        if(watermarkBytes != null) {            
            saveWatermarkContent(clientInstanceId, documentId, watermarkBytes);
        }

        ContentHandlerResult result = new ContentHandlerResult();
        result.put(ContentHandlerResult.DOCUMENT_ID_TO_RELOAD, documentId);
        return result;
    }

    /**
     * This is a helper function that demonstrates how to determine which pages have annotation objects on them. This
     * method should only be called in the case where your business logic requires having this information as it is not
     * required in the normal flow of simply saving annotations.
     *
     * @param layerBytes the data passed from the client representing the an annotation layer.
     * @param pageCount the number of pages in the document being saved.
     */
    private void logAnnPages(String layerId, byte[] layerBytes, int pageCount) {
        logger.trace("Logging page by page annotion info for layer {}", layerId);
        for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
            SnowAnn pageAnn = readPageLayerAnn(layerBytes, pageIndex);
            if (pageAnn != null) {
                logger.trace("There is at least one object on pageIndex=={}", pageIndex);
            } else {
                logger.trace("No objects on pageIndex=={}", pageIndex);
            }
        }
    }

    /**
     * @param layerbytes the data passed from the client representing the an annotation layer.
     * @param pageIndex the index of the page to read the annotation layer from.
     * @return a SnowAnn object representing the annotation layer.
     */
    protected static SnowAnn readPageLayerAnn(byte[] layerbytes, int pageIndex) {
        SnowAnn pageLayerAnn = new VirtualViewerSnowAnn();
        pageLayerAnn.SANN_read_ann(new DataInputStream(new ByteArrayInputStream(layerbytes)), pageIndex);
        pageLayerAnn.EnableProperties = true;
        pageLayerAnn.EnableEditText = true;
        if (pageLayerAnn.ann_fcs != null) {
            return pageLayerAnn;
        }
        return null;
    }

    private void deleteUnsavedExistingLayers(String documentId, String[] deletedLayers)
            throws VirtualViewerAPIException {
        if (deletedLayers == null || deletedLayers.length == 0) {
            return;
        }
        
        for (String deleteLayerId : deletedLayers) {
            if (deleteLayerId == null || deleteLayerId.isEmpty()) {
                continue;
            }
            logger.trace("About to delete layer: {}", StringEscapeUtils.escapeJava(deleteLayerId));
            deleteAnnotationLayer(documentId, deleteLayerId);
        }
    }

    private void deleteAnnotationLayer(String documentId, String layerId) {
        String fullFilePath = documentId + "." + layerId + ".ann";
        S3Handler s3Connector = new S3Handler(AwsAccessKeyId, AwsSecretAccessKey, s3RegionName);

        try {
            if(s3Connector.doesS3FileExist(s3BucketName, s3FolderName, fullFilePath)) {
                s3Connector.deleteFileFromS3(fullFilePath, s3BucketName, s3FolderName);
                logger.trace("Deleted layer: {}", StringEscapeUtils.escapeJava(layerId));
            } else {
                logger.trace("Layer {} does not exist and could not be deleted", StringEscapeUtils.escapeJava(layerId));
            }
        
        } catch (AmazonS3Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @throws com.snowbound.contenthandler.VirtualViewerAPIException can throw VirtualViewer exception to raise and log an error
     * @see com.snowbound.contenthandler.interfaces.VirtualViewerContentHandlerInterface#saveDocumentComponentsAs(ContentHandlerInput)
     */
    @Override
    public ContentHandlerResult saveDocumentComponentsAs(ContentHandlerInput input)
            throws VirtualViewerAPIException {
        if (readOnlyMode) {
            throw new VirtualViewerAPIException(READ_ONLY_ERROR_MESSAGE);
        }

        return saveDocumentComponents(input);
    }

    /* The example content handler uses filenames rather than GUIDs or another unique reference to a document,
     * so there's a chance that two emails might have an attachment named "attachment 1.pdf", for example. This
     * utility builds a more unique filename for the created email attachment. Note that the suggested document
     * ID of an email attachment is simply the name of the email attachment.
     */
    private String getEmailAttachmentDocumentId(ContentHandlerInput input, String attachmentName) {
        return "Attachment - " + input.getParentDocumentId() + "-" + attachmentName;
    }

    /**
     * @see com.snowbound.contenthandler.interfaces.VirtualViewerContentHandlerInterface#saveDocumentContent(ContentHandlerInput)
     */
    @Override
    public ContentHandlerResult saveDocumentContent(ContentHandlerInput input)
            throws VirtualViewerAPIException {
        if (readOnlyMode) {
            throw new VirtualViewerAPIException(READ_ONLY_ERROR_MESSAGE);
        }

        HttpServletRequest request = input.getHttpServletRequest();
        String clientInstanceId = input.getClientInstanceId();
        String documentKey = scrapeFileNameFromKey(input.getDocumentId());
        if (input.getIsEmailAttachment()) {
            documentKey = getEmailAttachmentDocumentId(input, documentKey);
        }
        byte[] data = input.getDocumentContent();
        return saveDocumentContent(request, clientInstanceId, documentKey, data);
    }

    private ContentHandlerResult saveDocumentContent(HttpServletRequest request,
            String clientInstanceId,
            String documentId,
            byte[] data)
            throws VirtualViewerAPIException {
        if (readOnlyMode) {
            throw new VirtualViewerAPIException(READ_ONLY_ERROR_MESSAGE);
        }

        if (data == null) {
            return null;
        }

        File saveFile = null;
        try {
            saveFile = File.createTempFile(documentId, ".tmp");
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
       
        ClientServerIO.saveFileBytes(data, saveFile);

        S3Handler s3Connector = new S3Handler(AwsAccessKeyId, AwsSecretAccessKey, s3RegionName);
        s3Connector.saveFileToS3(documentId, saveFile, s3BucketName, s3FolderName);

        ContentHandlerResult result = new ContentHandlerResult();
        result.put(ContentHandlerResult.DOCUMENT_ID_TO_RELOAD, documentId);
        return result;
    }

    private ContentHandlerResult saveDocumentContent(HttpServletRequest request,
            String clientInstanceId,
            String documentId,
            File inputfile)
            throws VirtualViewerAPIException {
        if (readOnlyMode) {
            throw new VirtualViewerAPIException(READ_ONLY_ERROR_MESSAGE);
        }

        ContentHandlerResult result = new ContentHandlerResult();
        if (inputfile == null) {
            return null;
        }

        File saveFile = null;
        try {
            saveFile = File.createTempFile(documentId, null);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            ClientServerIO.copyFile(inputfile, saveFile);
        } catch (IOException e) {
            logger.error("Error saving updated file", e);
            throw new VirtualViewerAPIException("Error saving updated file", e);
        }

        S3Handler s3Connector = new S3Handler(AwsAccessKeyId, AwsSecretAccessKey, s3RegionName);
        s3Connector.saveFileToS3(documentId, saveFile, s3BucketName, s3FolderName);

        result.put(ContentHandlerResult.DOCUMENT_ID_TO_RELOAD, documentId);
        return result;
    }

    /**
     * @see com.snowbound.contenthandler.interfaces.AnnotationsInterface#getAnnotationContent(ContentHandlerInput)
     */
    @Override
    public ContentHandlerResult getAnnotationContent(ContentHandlerInput input)
            throws VirtualViewerAPIException {
        // Note,  we should never be called with getAnnotationContent for the tiff tag layer.
        // the Content server will handle this without dependency on the content handler.
        return getAnnotationContentFromFile(input);
    }

    public ContentHandlerResult getAnnotationContentFromFile(ContentHandlerInput input)
            throws VirtualViewerAPIException {
        //String clientInstanceId = input.getClientInstanceId();
        String documentKey = scrapeFileNameFromKey(input.getDocumentId());
        String annotationKey = input.getAnnotationId();
        logger.trace("getAnnotationContent()");
        String fullFilePath = documentKey + "." + annotationKey + ".ann";

        S3Handler s3Connector = new S3Handler(AwsAccessKeyId, AwsSecretAccessKey, s3RegionName);

        logger.trace("Retrieving annotation file: {}", StringEscapeUtils.escapeJava(fullFilePath));
        Map props = null;
        ContentHandlerResult propsResult = getAnnotationProperties(input);
        
        if (propsResult != null) {
            props = propsResult.getAnnotationProperties();
            
            //Content handler can retrieve permission level and redaction flag
            Boolean redactionFlag = (Boolean) props.get(AnnotationLayer.PROPERTIES_KEY_REDACTION_FLAG);
            Integer permissionLevel = (Integer) props.get(AnnotationLayer.PROPERTIES_KEY_PERMISSION_LEVEL);            
        }
        
        try {
            byte[] bytes = s3Connector.getFileS3Bytes(fullFilePath, s3BucketName, s3FolderName);
            ContentHandlerResult result = new ContentHandlerResult();
            result.put(ContentHandlerResult.KEY_ANNOTATION_CONTENT, bytes);
            result.put(ContentHandlerResult.KEY_ANNOTATION_DISPLAY_NAME, input.getAnnotationId());
            result.put(ContentHandlerResult.KEY_ANNOTATION_PROPERTIES, props);
            return result;
        } catch (IOException e) {
            logger.error("Failed to retrieve annotation content for the layer {}", StringEscapeUtils.escapeJava(fullFilePath), e);
            return null;
        }
    }

    /**
     * 
     * @throws VirtualViewerAPIException can throw VirtualViewer exception to raise and log an error 
     * @see com.snowbound.contenthandler.interfaces.CreateDocumentInterface#createDocument(com.snowbound.contenthandler.ContentHandlerInput) 
     */
    @Override
    public ContentHandlerResult createDocument(ContentHandlerInput input)
            throws VirtualViewerAPIException {
        String documentId = scrapeFileNameFromKey(input.getDocumentId());
        
        if (input.getIsEmailAttachment()) {
            documentId = getEmailAttachmentDocumentId(input, documentId);
        }

        S3Handler s3Connector = new S3Handler(AwsAccessKeyId, AwsSecretAccessKey, s3RegionName);

        if (s3Connector.doesS3FileExist(s3BucketName, s3FolderName, documentId)) {
            if (input.getIsEmailAttachment()) {
                // In this case, if a user is attempting to open an email attachment that has been opened before, we can simply
                // recognize that and return the appropriate document ID without error.
                ContentHandlerResult result = new ContentHandlerResult();
                result.put(ContentHandlerResult.DOCUMENT_ID_TO_RELOAD, documentId);
                return result;
            }
            throw new VirtualViewerAPIException("A document by this name already exists. Please change the name and try again.");
        }

        return saveDocumentContent(input);
    }


    /**
     * @see com.snowbound.contenthandler.interfaces.AnnotationsInterface#getAnnotationProperties(ContentHandlerInput)
     */
    @Override
    public ContentHandlerResult getAnnotationProperties(ContentHandlerInput input)
            throws VirtualViewerAPIException {
        HttpServletRequest request = input.getHttpServletRequest();
        String clientInstanceId = input.getClientInstanceId();
        int permissionLevel = PermissionLevel.DELETE;

        // clientInstanceId can be set to override annotation permission levels.
        if(clientInstanceId != null && !clientInstanceId.isEmpty()) {
            try {
                JSONObject debugSettings = new JSONObject(clientInstanceId);
                permissionLevel = debugSettings.getInt("annotationPermissionLevel");
            }
            catch(JSONException e) {
                // malformed or not JSON
            }
        }

        String documentKey = (input.getDocumentId());
        String annotationKey = input.getAnnotationId();
        logger.trace("getAnnotationProperties()");
        Hashtable properties = new Hashtable();
        String baseAnnFilename = documentKey + "." + annotationKey;
        String annFilename = baseAnnFilename + ".ann";
        

        S3Handler s3Connector = new S3Handler(AwsAccessKeyId, AwsSecretAccessKey, s3RegionName);

        if (s3Connector.doesS3FileExist(s3BucketName, s3FolderName, annFilename)) {
            properties.put(AnnotationLayer.PROPERTIES_KEY_PERMISSION_LEVEL, permissionLevel);
            properties.put(AnnotationLayer.PROPERTIES_KEY_REDACTION_FLAG, false);
        }

        ContentHandlerResult result = new ContentHandlerResult();
        result.put(ContentHandlerResult.KEY_ANNOTATION_PROPERTIES, properties);
        return result;
    }

    /**
     * @see com.snowbound.contenthandler.interfaces.BookmarksInterface#getBookmarkContent(ContentHandlerInput)
     */
    @Override
    public ContentHandlerResult getBookmarkContent(ContentHandlerInput input)
            throws VirtualViewerAPIException {
        String clientInstanceId = input.getClientInstanceId();
        String documentKey = scrapeFileNameFromKey(input.getDocumentId());
        logger.trace("GetBookmarkContent: clientInstanceId {}", StringEscapeUtils.escapeJava(clientInstanceId));
        String bookmarkFilename = documentKey + ".bookmarks.xml";
        String fullFilePath =  bookmarkFilename;

        S3Handler s3Connector = new S3Handler(AwsAccessKeyId, AwsSecretAccessKey, s3RegionName);
        byte[] bytes = null;

        try {
            logger.trace("Retrieving bookmark file: {}", StringEscapeUtils.escapeJava(fullFilePath));
            bytes = s3Connector.getFileS3Bytes(fullFilePath, s3BucketName, s3FolderName);
            ContentHandlerResult result = new ContentHandlerResult();
            result.put(ContentHandlerResult.KEY_BOOKMARK_CONTENT, bytes);
            return result;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        } catch (AmazonS3Exception e) {
            e.printStackTrace();
            return null;
        }
    } 
    
    /**
     * @throws VirtualViewerAPIException can throw VirtualViewer exception to raise and log an error 
     * @see com.snowbound.contenthandler.interfaces.BookmarksInterface#deleteBookmarkContent(com.snowbound.contenthandler.ContentHandlerInput) 
     */
    @Override
    public ContentHandlerResult deleteBookmarkContent(ContentHandlerInput input) 
            throws VirtualViewerAPIException {
        String clientInstanceId = input.getClientInstanceId();
        String documentId = scrapeFileNameFromKey(input.getDocumentId());

        return saveBookmarkContent(clientInstanceId, documentId, null);
    }

    private ContentHandlerResult saveBookmarkContent(String clientInstanceId, String documentId, byte[] data) {
        logger.trace("saveBookmarkContent...{} clientInstanceId: {}", StringEscapeUtils.escapeJava(documentId), StringEscapeUtils.escapeJava(clientInstanceId));
        String fullFilePath =  documentId + ".bookmarks.xml";
        
        S3Handler s3Connector = new S3Handler(AwsAccessKeyId, AwsSecretAccessKey, s3RegionName);
       
        if (data == null) {
            if (s3Connector.doesS3FileExist(s3BucketName, s3FolderName, fullFilePath)) {
                try{
                    s3Connector.deleteFileFromS3(fullFilePath, s3BucketName, s3FolderName);
                } catch (AmazonS3Exception a) {
                    a.printStackTrace();
                }
            }
            return ContentHandlerResult.VOID;
        }
        try {
            File file = File.createTempFile(fullFilePath, null);
            ClientServerIO.saveFileBytes(data, file);
            s3Connector.saveFileToS3(fullFilePath, file, s3BucketName, s3FolderName);
        } catch (Exception ex) {
            logger.error("Error while saving bookmark content to file: {}", ex.getMessage());
        }
        return ContentHandlerResult.VOID;
    }

    /**
     * @throws com.snowbound.contenthandler.VirtualViewerAPIException can throw VirtualViewer exception to raise and log an error
     * @see com.snowbound.contenthandler.interfaces.WatermarksInterface#getWatermarkContent(ContentHandlerInput)
     */
    @Override
    public ContentHandlerResult getWatermarkContent(ContentHandlerInput input)
            throws VirtualViewerAPIException {
        String clientInstanceId = input.getClientInstanceId();
        String documentKey = scrapeFileNameFromKey(input.getDocumentId());
        logger.trace("getWatermarkContent: clientInstanceId {}", StringEscapeUtils.escapeJava(clientInstanceId));
        String fullFilePath = documentKey + ".watermarks.json";
        S3Handler s3Connector = new S3Handler(AwsAccessKeyId, AwsSecretAccessKey, s3RegionName);

        byte[] content = null;
    
        try {
            content = s3Connector.getFileS3Bytes(fullFilePath, s3BucketName, s3FolderName);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        } catch (AmazonS3Exception e) {
            e.printStackTrace();
            return null;
        }

        logger.trace("Retrieving watermark file: {}", StringEscapeUtils.escapeJava(fullFilePath));
        ContentHandlerResult result = new ContentHandlerResult();
        result.put(ContentHandlerResult.KEY_WATERMARK_CONTENT, content);
        return result;

    }  
    
    /**
     * @throws VirtualViewerAPIException can throw VirtualViewer exception to raise and log an error 
     * @see com.snowbound.contenthandler.interfaces.WatermarksInterface#deleteWatermarkContent(ContentHandlerInput)
     */
    @Override
    public ContentHandlerResult deleteWatermarkContent(ContentHandlerInput input) 
        throws VirtualViewerAPIException {
        String clientInstanceId = input.getClientInstanceId();
        String documentId = scrapeFileNameFromKey(input.getDocumentId());
        return saveWatermarkContent(clientInstanceId, documentId, null);
    }

    /**
     * @param request
     * @param clientInstanceId
     * @param documentKey
     * @param data
     * @return
     */
    private ContentHandlerResult saveWatermarkContent(String clientInstanceId, String documentId, byte[] data) {
        logger.trace("saveWatermarkContent...{} clientInstanceId: {}", StringEscapeUtils.escapeJava(documentId), StringEscapeUtils.escapeJava(clientInstanceId));
        String fullFilePath =  documentId + ".watermarks.json";

        S3Handler s3Connector = new S3Handler(AwsAccessKeyId, AwsSecretAccessKey, s3RegionName);

        if (data == null) {
            if (s3Connector.doesS3FileExist(s3BucketName, s3FolderName, fullFilePath)) {
                try{
                    s3Connector.deleteFileFromS3(fullFilePath, s3BucketName, s3FolderName);
                } catch (AmazonS3Exception a) {
                    a.printStackTrace();
                }
            }
            return ContentHandlerResult.VOID;
        }
        try {
            File file = File.createTempFile(fullFilePath, null);
            ClientServerIO.saveFileBytes(data, file);
            s3Connector.saveFileToS3(fullFilePath, file, s3BucketName, s3FolderName);
        } catch (Exception ex) {
            logger.error("Error while saving bookmark content to file: {}", ex.getMessage());
        }

        return ContentHandlerResult.VOID;
    }

    /**
     * @throws com.snowbound.contenthandler.VirtualViewerAPIException can throw VirtualViewer exception to raise and log an error
     * @see com.snowbound.contenthandler.interfaces.DocumentNotesInterface#getNotesContent(ContentHandlerInput)
     */
    @Override
    public ContentHandlerResult getNotesContent(ContentHandlerInput input)
            throws VirtualViewerAPIException {
        String clientInstanceId = input.getClientInstanceId();
        String documentKey = scrapeFileNameFromKey(input.getDocumentId());
        logger.trace("getNotesContent: clientInstanceId {}", StringEscapeUtils.escapeJava(clientInstanceId));
        String fullFilePath = documentKey + ".notes.xml";

        S3Handler s3Connector = new S3Handler(AwsAccessKeyId, AwsSecretAccessKey, s3RegionName);
        byte[] bytes = null;
    
        try {
            bytes = s3Connector.getFileS3Bytes(fullFilePath, s3BucketName, s3FolderName);
            ContentHandlerResult result = new ContentHandlerResult();
            result.put(ContentHandlerResult.KEY_NOTES_CONTENT, bytes);
            return result;
        } catch (AmazonS3Exception e) {
            e.printStackTrace();
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
    
    /**
     * @throws VirtualViewerAPIException can throw VirtualViewer exception to raise and log an error
     * @see com.snowbound.contenthandler.interfaces.DocumentNotesInterface#deleteNotesContent(ContentHandlerInput)
     */
    @Override
    public ContentHandlerResult deleteNotesContent(ContentHandlerInput input)
            throws VirtualViewerAPIException {        
        String clientInstanceId = input.getClientInstanceId();
        String documentId = scrapeFileNameFromKey(input.getDocumentId());
        
        return saveNotesContent(clientInstanceId, documentId, null);
    }

    /**
     * @param request - the HttpServletRequest
     * @param clientInstanceId - the client instance id
     * @param documentKey - the document key
     * @param data - the data to save
     * @return - ContentHandlerResult
     */
    private ContentHandlerResult saveNotesContent(String clientInstanceId, String documentId, byte[] data) {
        logger.trace("saveNotesContent...{} clientInstanceId: {}", StringEscapeUtils.escapeJava(documentId), StringEscapeUtils.escapeJava(clientInstanceId));
        
        String fullFilePath = documentId + ".notes.xml";
        S3Handler s3Connector = new S3Handler(AwsAccessKeyId, AwsSecretAccessKey, s3RegionName);

    
        if (data == null) {
            if (s3Connector.doesS3FileExist(s3BucketName, s3FolderName, fullFilePath)) {
                try{
                    s3Connector.deleteFileFromS3(fullFilePath, s3BucketName, s3FolderName);
                } catch (AmazonS3Exception a) {
                    a.printStackTrace();
                }
            } else {
                return ContentHandlerResult.VOID;
            }
            return ContentHandlerResult.VOID;
        }

        try {
            File saveFile = File.createTempFile(fullFilePath, null);
            ClientServerIO.saveFileBytes(data, saveFile);
            s3Connector.saveFileToS3(fullFilePath, saveFile, s3BucketName, s3FolderName);
        } catch (Exception e) {
            logger.error("Error while saving note content to file:", e);
        }
        return new ContentHandlerResult();
    }

    /**
     * @see com.snowbound.contenthandler.interfaces.VirtualViewerContentHandlerInterface#getDocumentContent(ContentHandlerInput)
     */
    @Override
    public ContentHandlerResult getDocumentContent(ContentHandlerInput input)
            throws VirtualViewerAPIException {
        //String clientInstanceId = input.getClientInstanceId();
        String key = scrapeFileNameFromKey(input.getDocumentId());

        ContentHandlerResult result = new ContentHandlerResult();
        
        // This is an example of how to use VV's "Sparse Document" mechanism.  It is not intended for production use.
        // The prefix SparseDocument: is used for testing; it is not required in the ID for Sparse Documents.
        if (key.startsWith(PREFIX_SPARSE_DOCUMENT)) {
            List<DataInputStream> vectorOfStreams = new ArrayList<>();

            int pageNumber = input.getSparseRequestedPageNumber();
            int pageCount = input.getSparseRequestedPageCount();

            String dirName = key.split(":")[1];

            logger.trace("getDocumentContent: Retrieving sparse document: {}", StringEscapeUtils.escapeJava(dirName));

            S3Handler s3Connector = new S3Handler(AwsAccessKeyId, AwsSecretAccessKey, s3RegionName);
            String s3path = "";
            if(s3FolderName == null || s3FolderName.isEmpty()) {
                s3path = dirName; 
            } else {
                s3path = s3FolderName + "/" + dirName;
            }

            String[] filesInDir = s3Connector.listS3BucketObjects(s3BucketName, s3path);
            if(filesInDir == null) {
                logger.error("SparseDocument not found: {}", StringEscapeUtils.escapeJava(key));
                throw new VirtualViewerAPIException("SparseDocument not found: " + key);
            }

            int startIndex = pageNumber;
            int endIndex = pageNumber + pageCount;
            if (endIndex > filesInDir.length || pageCount == 0) {
                endIndex = filesInDir.length;
            }

            for (int x = startIndex; x < endIndex; x++) {
                String file = filesInDir[x];
                if(!file.isEmpty() || file != null) {
                    try {
                        DataInputStream documentContent = s3Connector.getFileS3DataInputStream(file, s3BucketName, s3path);
                        if(documentContent == null) {
                            logger.error("Document not found: {}", StringEscapeUtils.escapeJava(file));
                        } else { 
                            vectorOfStreams.add(documentContent);
                        }
                        
                    } catch (AmazonS3Exception e) {                    
                        logger.error("Error while retrieving document content from file", e);
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }

            result.put(ContentHandlerResult.KEY_DOCUMENT_SPARSE_ELEMENTS, vectorOfStreams);
            result.put(ContentHandlerResult.KEY_DOCUMENT_SPARSE_PAGE_INDEX, pageNumber);
            result.put(ContentHandlerResult.KEY_DOCUMENT_SPARSE_RETURN_PAGE_COUNT, vectorOfStreams.size());
            result.put(ContentHandlerResult.KEY_DOCUMENT_SPARSE_TOTAL_PAGE_COUNT, vectorOfStreams.size());

            result.put(ContentHandlerResult.KEY_DOCUMENT_DISPLAY_NAME, dirName);

            if (contentHandlerDebug) {
                String reversed = new StringBuilder(key).reverse().toString().toUpperCase();

                result.put(ContentHandlerResult.KEY_DOCUMENT_DISPLAY_NAME, reversed);
            }
        } // This is an example of how to use VV's Content Elements mechanism.  It is not intended for production use.
        // The prefix CompountDocument: is used for testing; it is not required in the ID for Compound Documents.
        else if (key.startsWith(PREFIX_COMPOUND_DOCUMENT)) {
            List<DataInputStream> vectorOfStreams = new ArrayList<>();

            String documentDefinition = key.substring(key.indexOf(':') + 1);
            StringTokenizer st = new StringTokenizer(documentDefinition, ",");
            S3Handler s3Connector = new S3Handler(AwsAccessKeyId, AwsSecretAccessKey, s3RegionName);
           
            while (st.hasMoreTokens()) {
                String documentComponent = st.nextToken();

                DataInputStream documentContent = null;

                try {
                    documentContent = s3Connector.getFileS3DataInputStream(key, s3BucketName, s3FolderName);
                } catch (FileNotFoundException fnfe) {
                    /* Removing stack trace here, as it was unnecessary */
                    logger.error("Document not found", fnfe);
                    throw new VirtualViewerAPIException("Document not found: " + ClientServerIO.makeXssSafe(key), fnfe);
                } catch (IOException e) {
                    logger.error("Could not read document file", e);
                    return null;
                }
                if(documentContent == null) {
                    logger.error("Document not found: {}", StringEscapeUtils.escapeJava(documentComponent));
                } else {
                    vectorOfStreams.add(documentContent);
                }
                
            }

            result.put(ContentHandlerResult.KEY_DOCUMENT_CONTENT_ELEMENTS, vectorOfStreams);
            result.put(ContentHandlerResult.KEY_DOCUMENT_DISPLAY_NAME, documentDefinition);

            if (contentHandlerDebug) {
                String reversed = new StringBuilder(key).reverse().toString().toUpperCase();
                result.put(ContentHandlerResult.KEY_DOCUMENT_DISPLAY_NAME, reversed);
            }
        } else {
            S3Handler s3Connector = new S3Handler(AwsAccessKeyId, AwsSecretAccessKey, s3RegionName);
            DataInputStream documentContent = null;
        
            try {
                logger.trace("Retrieving document file: {}", StringEscapeUtils.escapeJava(key));
                documentContent = s3Connector.getFileS3DataInputStream(key, s3BucketName, s3FolderName);
            } catch (AmazonS3Exception e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

            if(documentContent == null) {
                logger.error("Document not found: {}", StringEscapeUtils.escapeJava(key));
                throw new VirtualViewerAPIException("Document not found: " + key);
            }

            logger.trace("getDocumentContent: Retrieving single document");

            try {
                result.put(ContentHandlerResult.KEY_DOCUMENT_INPUT_STREAM, documentContent);

                if (contentHandlerDebug) {
                    String reversed = new StringBuilder(key).reverse().toString();

                    result.put(ContentHandlerResult.KEY_DOCUMENT_DISPLAY_NAME, reversed);
                }
            } catch (Exception e) {
                /* Removing stack trace here, as it was unnecessary */
                logger.error("Document not found", e);
                throw new VirtualViewerAPIException("Document not found: " + ClientServerIO.makeXssSafe(key), e);
            } 
        }
        
        return result;
    }

    /**
     * @throws com.snowbound.contenthandler.VirtualViewerAPIException can throw VirtualViewer exception to raise and log an error
     * @see com.snowbound.contenthandler.interfaces.AnnotationsInterface#deleteAnnotation(ContentHandlerInput)
     */
    @Override
    public ContentHandlerResult deleteAnnotation(ContentHandlerInput input)
            throws VirtualViewerAPIException {
        if (readOnlyMode) {
            throw new VirtualViewerAPIException(READ_ONLY_ERROR_MESSAGE);
        }

        //String clientInstanceId = input.getClientInstanceId();
        String documentKey = scrapeFileNameFromKey(input.getDocumentId());
        String annotationKey = input.getAnnotationId();
        String annotationFilename = documentKey + "." + annotationKey + ".ann";
        String fullFilePath =  annotationFilename;

        S3Handler s3Connector = new S3Handler(AwsAccessKeyId, AwsSecretAccessKey, s3RegionName);
        logger.trace("Deleting annotation file: {}", StringEscapeUtils.escapeJava(fullFilePath));
        try {
            s3Connector.deleteFileFromS3(annotationFilename, s3BucketName, s3FolderName);
        } catch (Exception e) {
            logger.error("Failed to delete layer {}", e);
        }
        return null;
    }

    private static boolean filenameHasKnownExtension(String filename,
            List<String> knownExtensions) {
        for (String knownExtension : knownExtensions) {
            if (filename.toUpperCase().endsWith("." + knownExtension.toUpperCase())) {
                return true;
            }
        }

        String[] otherExtensions
                = {"txt", "jb2"};
        for (String otherExtension : otherExtensions) {
            if (filename.toUpperCase().endsWith("." + otherExtension.toUpperCase())) {
                return true;
            }
        }
        return false;
    }

    /**
     * @throws VirtualViewerAPIException can throw VirtualViewer exception to raise and log an error
     * @see com.snowbound.contenthandler.interfaces.EventSubscriberInterface#eventNotification(com.snowbound.contenthandler.ContentHandlerInput) 
     */
    @Override
    public ContentHandlerResult eventNotification(ContentHandlerInput input)
            throws VirtualViewerAPIException {
        logger.trace("FileContentHandler.eventNotification");
        Iterator paramIterator = input.keySet().iterator();
        String eventType = (String) input.get(ContentHandlerResult.KEY_EVENT);
        if (eventType.equals(ContentHandlerResult.VALUE_EVENT_DOCUMENT_RETRIEVED_FROM_CACHE)) {
            //code can be written like this to deal with a specific type of event
        } else {
            while (paramIterator.hasNext()) {
                Object key = paramIterator.next();
                if (!key.equals(ContentHandlerInput.KEY_CLIENT_INSTANCE_ID) && !key.equals(ContentHandlerInput.KEY_HTTP_SERVLET_REQUEST)) {
                    try {
                        String value = (String) input.get(key);
                        logger.trace("Key: {}, value: {}", StringEscapeUtils.escapeJava((String)key), StringEscapeUtils.escapeJava(value));
                    } catch(ClassCastException e) {
                        logger.error("Event notification received an unexpected parameter", e);
                    }
                }
            }
        }
        return ContentHandlerResult.VOID;
    }

    public static void setSupportsTiffTagAnnotations(boolean pValue) {
        gSupportTiffTagAnnotations = pValue;
    }

    @Override
    public ContentHandlerResult getAllAnnotationsForDocument(
            ContentHandlerInput input) throws VirtualViewerAPIException {
        ContentHandlerResult result = new ContentHandlerResult();

        Map<String, AnnotationLayer> annotationHash = new LinkedHashMap<>();

        String documentId = scrapeFileNameFromKey(input.getDocumentId());
        String clientInstanceId = input.getClientInstanceId();

        ContentHandlerResult annResult = this.getAnnotationNames(input);
        String[] annNames = annResult.getAnnotationNames();

        if (annNames != null) {
            for (String annotationId : annNames) {
                ContentHandlerInput cInput = new ContentHandlerInput(documentId, clientInstanceId);

                cInput.setAnnotationId(annotationId);

                ContentHandlerResult cResult = this.getAnnotationContent(cInput);
                if (cResult == null) {
                    continue;
                }
                byte[] bytes = (byte[]) cResult.get(ContentHandlerResult.KEY_ANNOTATION_CONTENT);
                String displayName = (String) cResult.get(ContentHandlerResult.KEY_ANNOTATION_DISPLAY_NAME);
                Map props = (Map) cResult.get(ContentHandlerResult.KEY_ANNOTATION_PROPERTIES);

                AnnotationLayer annoLayer = new AnnotationLayer();

                annoLayer.setData(bytes);
                annoLayer.setDocumentId(documentId);
                annoLayer.setLayerName(displayName);
                annoLayer.setLayerObjectId(annotationId);
                annoLayer.setModified(false);
                annoLayer.setNew(false);
                annoLayer.setProperties(props);

                annotationHash.put(annotationId, annoLayer);
            }
        }

        result.put(ContentHandlerResult.KEY_ALL_ANNOTATIONS_HASH, annotationHash);

        return result;
    }    

    @Override
    public ContentHandlerResult getOCRDataForDocument(ContentHandlerInput input) throws VirtualViewerAPIException {
        return retrieveOCRDataFile(input);
    }
    
    // In the example content handler, both getOCRDataForDocument and getOCRDataOnPerformOCR will 
    // return the same file with the same mechanism, so use this helper function. 
    // In product, getOCRDataForDocument should only return existing OCR data to avoid 
    // impacting document retrieval performance, while getOCRDataOnPerformOCR can either return existing OCR data or run an 
    // OCR engine as it is run on-demand.
    private ContentHandlerResult retrieveOCRDataFile(ContentHandlerInput input) throws VirtualViewerAPIException {
        String clientInstanceId = input.getClientInstanceId();
        String documentKey = scrapeFileNameFromKey(input.getDocumentId());
        logger.trace("getOCRDataForDocument: clientInstanceId {}", StringEscapeUtils.escapeJava(clientInstanceId));
        String ocrDataFilename = documentKey + ".ocr-text.json";
    
        S3Handler s3Connector = new S3Handler(AwsAccessKeyId, AwsSecretAccessKey, s3RegionName);

        byte[] ocrData = null;
        try {
            ContentHandlerResult result = new ContentHandlerResult();
            ocrData = s3Connector.getFileS3Bytes(ocrDataFilename, s3BucketName, s3FolderName);
            result.put(ContentHandlerResult.KEY_OCR_DATA_JSON, ocrData);
            logger.trace("Retrieving OCR data file: {}", StringEscapeUtils.escapeJava(ocrDataFilename));
            return result;
        } catch (AmazonS3Exception | IOException e) {
            e.printStackTrace();
            logger.trace("No OCR data found for: {}", StringEscapeUtils.escapeJava(documentKey));
            return null;
        }

    }
    
    @Override
    public ContentHandlerResult getOCRDataOnPerformOCR(ContentHandlerInput input)
            throws VirtualViewerAPIException {
        return retrieveOCRDataFile(input);
    }
    
    @Override
    public ContentHandlerResult validateCache(ContentHandlerInput input) {
        // This is a trivial implemenation of validateCache that just allows it to be tested a little more easily.
        ContentHandlerResult result = new ContentHandlerResult();
        result.put(ContentHandlerResult.KEY_USE_OF_CACHE_ALLOWED, Boolean.TRUE);
        return result;
    }

    @Override
    public ContentHandlerResult checkAvailable(ContentHandlerInput input) throws VirtualViewerAPIException {
        return new ContentHandlerResult();
    }

    private String scrapeFileNameFromKey(String key) throws VirtualViewerAPIException{
        String prefix = ""; 
        if (key.startsWith(PREFIX_SPARSE_DOCUMENT)) {
            prefix = PREFIX_SPARSE_DOCUMENT;
        } else if (key.startsWith(PREFIX_COMPOUND_DOCUMENT)) {
            prefix = PREFIX_COMPOUND_DOCUMENT;
        } else if (key.startsWith("VirtualDocument:")) {
            prefix = "VirtualDocument:";
        } else if (key.startsWith("IncludesExternalReferences:")) {
            prefix = "IncludesExternalReferences:";
        }
        key = key.substring(prefix.length());
        try {
            Path keyPath = Paths.get(key);
            return prefix + keyPath.getFileName().toString();
        } catch (InvalidPathException ipe) {
            logger.error("The provided document key {} was invalid and may have contained a relative path name", StringEscapeUtils.escapeJava(key));
            throw new VirtualViewerAPIException("An exception occurred: the provided document key is invalid", ipe);
        }        
    }

    private class FileContentHandlerFileNameFilter implements FilenameFilter
    {
        @Override
        public boolean accept(File dir, String filename) {
            String lFileName = filename.toLowerCase();

            return !(lFileName.endsWith(".ann") ||
                     lFileName.endsWith(".ds_store") ||
                     lFileName.endsWith(".ocr-text.json"));
        }
    }
}
