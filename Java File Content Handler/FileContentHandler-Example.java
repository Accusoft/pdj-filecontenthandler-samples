/**
 * Copyright (C) 2002, 2003, 2021 by Snowbound Software Corp. All rights reserved.
 * This example should be compatible with VirtualViewer GA releases 5.3.0 - 5.6.0
 */
package com.snowbound.virtualviewer.contenthandler.example;

import Snow.Format;
import Snow.SnowAnn;
import com.snowbound.common.transport.AnnotationLayer;
import com.snowbound.common.transport.ExternalReference;
import com.snowbound.common.transport.PermissionLevel;
import com.snowbound.common.transport.VirtualViewerSnowAnn;
import com.snowbound.common.utils.ClientServerIO;
import com.snowbound.common.utils.RasterMaster;
import com.snowbound.common.utils.SnowLoggerFactory;
import com.snowbound.contenthandler.ContentHandlerInput;
import com.snowbound.contenthandler.ContentHandlerResult;
import com.snowbound.contenthandler.VirtualViewerAPIException;
import com.snowbound.contenthandler.VirtualViewerFormatHash;
import com.snowbound.contenthandler.interfaces.AllAnnotationsInterface;
import com.snowbound.contenthandler.interfaces.AnnotationsInterface;
import com.snowbound.contenthandler.interfaces.AvailableDocumentsInterface;
import com.snowbound.contenthandler.interfaces.BookmarksInterface;
import com.snowbound.contenthandler.interfaces.CacheValidator;
import com.snowbound.contenthandler.interfaces.CreateDocumentInterface;
import com.snowbound.contenthandler.interfaces.DocumentNotesInterface;
import com.snowbound.contenthandler.interfaces.EventSubscriberInterface;
import com.snowbound.contenthandler.interfaces.OCRDataProviderInterface;
import com.snowbound.contenthandler.interfaces.SendDocumentInterface;
import com.snowbound.contenthandler.interfaces.VirtualViewerContentHandlerInterface;
import com.snowbound.contenthandler.interfaces.WatermarksInterface;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import org.slf4j.Logger;

/**
 * This is a sample content handler for demonstration and example purposes. It saves documents and sub-components to the
 * local file system. It should not be used for production instances of VirtualViewer.
 */
public class FileContentHandler
        implements VirtualViewerContentHandlerInterface, AvailableDocumentsInterface,
        AllAnnotationsInterface, AnnotationsInterface, BookmarksInterface, CreateDocumentInterface, 
        DocumentNotesInterface, EventSubscriberInterface, SendDocumentInterface, WatermarksInterface,
        OCRDataProviderInterface, CacheValidator
{

    /**
     *
     */
    protected static final String PARAM_FILE_PATH = "filePath";

    /**
     *
     */
    protected static final String PARAM_TIFF_TAG_ANNOTATIONS = "tiffTagAnnotations";

    /**
     *
     */
    protected static final String PARAM_CONTENT_HANDLER_DEBUG = "contentHandlerDebugMode";

    /**
     *
     */
    protected static final String PARAM_READ_ONLY_MODE = "fileContentHandlerReadOnlyMode";

    /**
     *
     */
    protected static String gFilePath = "c:/imgs/";

    private static final String PREFIX_SPARSE_DOCUMENT = "SparseDocument:";
    private static final String PREFIX_COMPOUND_DOCUMENT = "CompoundDocument:";
    private static final String READ_ONLY_ERROR_MESSAGE = "Saving has been disabled by the administrator";
    private boolean readOnlyMode = false;
    private static boolean gSupportTiffTagAnnotations = false;
    private static boolean contentHandlerDebug = false;
    
    private static final Logger logger = SnowLoggerFactory.getLogger(FileContentHandler.class);

    @Override
    public void init(ServletConfig config) throws VirtualViewerAPIException {        
        
        logger.info("Loading file content handler example");
	logger.info("NOT RECOMMENDED FOR PRODUCTION USE");
        
        String pathParam = config.getInitParameter(PARAM_FILE_PATH);
        if (pathParam != null) {
            setFilePath(pathParam, config.getServletContext());
        }

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

    public static void setFilePath(String pathParam, ServletContext context) {
        if ((pathParam.startsWith("./") || pathParam.startsWith(".\\")) && context != null) {
            pathParam = pathParam.replace("./", "/");
            pathParam = pathParam.replace(".\\\\", "/");

            gFilePath = context.getRealPath(pathParam) +
                     File.separator;
        } else {
            gFilePath = pathParam;
        }
        
        logger.info("File path for documents is configured to {}", gFilePath);        
    }

    /**
     * @throws com.snowbound.contenthandler.VirtualViewerAPIException can throw VirtualViewer exception to raise and log an error
     * @see com.snowbound.contenthandler.interfaces.AnnotationsInterface#getAnnotationNames(ContentHandlerInput)
     */
    @Override
    public ContentHandlerResult getAnnotationNames(ContentHandlerInput input)
            throws VirtualViewerAPIException {
        String clientInstanceId = input.getClientInstanceId();
        String documentKey = input.getDocumentId();
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

        String annPath = gFilePath;
        File imgDirectory = new File(annPath);
        String[] files = imgDirectory.list();
        for (String fileName : files) {
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
        result.put(ContentHandlerResult.KEY_ANNOTATION_NAMES,
                arrayAnnotationIds);
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

    private byte[] getTiffTagAnnotations(File documentFile) {
        RasterMaster snow = new RasterMaster();
        int[] value = new int[1];
        byte[] buff = new byte[500000];
        int WANG_ANNOTATION_TAG_ID = 32932;
        snow.IMGLOW_get_tiff_tag(WANG_ANNOTATION_TAG_ID,
                buff.length,
                value,
                documentFile.getAbsolutePath(),
                buff,
                0);
        return buff;
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
        String documentKey = input.getDocumentId();
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
        String baseFilePath = gFilePath + documentKey + "." + annotationKey +
                 pageIndexPortion;
        String annFilePath = baseFilePath + ".ann";
        
        String fullFilePath = annFilePath;
        if (annProperties != null) {
            //Content handler can retrieve permission level to implement different save behavior
            Boolean redactionFlag = (Boolean) annProperties
                    .get(AnnotationLayer.PROPERTIES_KEY_REDACTION_FLAG);
            Integer permissionLevel = (Integer) annProperties
                    .get(AnnotationLayer.PROPERTIES_KEY_PERMISSION_LEVEL);
        }
        // Make sure any existing ann files are deleted
        File file = new File(annFilePath);
        if (file.exists()) {
            file.delete();
        }
        
        logger.trace("saveAnnotationContent: saving {}", annotationKey);
        file = new File(fullFilePath);
        try {
            if (data.length > 0) {
                ClientServerIO.saveFileBytes(data, file);
            }
        } catch (Exception e) {
            logger.error("Exception while saving annotation {} to file at {}", annotationKey, file.getPath(), e);
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
        String documentId = input.getDocumentId();
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
                    logger.trace("Skipping unmodified Layer: {}", annLayer.getLayerName());
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
     * @param layerbytes
     * @param pageIndex
     * @return
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
            logger.trace("About to delete layer: {}", deleteLayerId);
            deleteAnnotationLayer(documentId, deleteLayerId);
        }
    }

    private void deleteAnnotationLayer(String documentId, String layerId) {
        String annotationFilename = documentId + "." + layerId + ".ann";
        String fullFilePath = gFilePath + annotationFilename;
        //String renamedFilename = documentId + "." + layerId + ".deleted-ann";
        //String fullRenamedFilePath = gFilePath + renamedFilename;
        //File renamedFile = new File (fullRenamedFilePath);
        File annFile = new File(fullFilePath);
        if (annFile.exists()) {
            /* The initial implementaion will be to actually delete the file.
             * But as you can see from the code, we could simply rename it 
             */
            //annFile.renameTo(renamedFile);
            annFile.delete();
            logger.trace("Deleted layer: {}", layerId);
        } else {
            logger.trace("Layer {} does not exist and could not be deleted", layerId);
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

    /**
     * 
     * @throws VirtualViewerAPIException can throw VirtualViewer exception to raise and log an error 
     * @see com.snowbound.contenthandler.interfaces.CreateDocumentInterface#createDocument(com.snowbound.contenthandler.ContentHandlerInput) 
     */
    @Override
    public ContentHandlerResult createDocument(ContentHandlerInput input)
            throws VirtualViewerAPIException {
        String documentId = input.getDocumentId();
        File saveFile = new File(gFilePath + documentId);
        if (saveFile.exists()) {
            throw new VirtualViewerAPIException("A document by this name already exists. Please change the name and try again.");
        }

        return saveDocumentContent(input);
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
        String documentKey = input.getDocumentId();
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
        File saveFile = new File(gFilePath + documentId);
        ClientServerIO.saveFileBytes(data, saveFile);
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
        File saveFile = new File(gFilePath + documentId);
        try {
            ClientServerIO.copyFile(inputfile, saveFile);
        } catch (IOException e) {
            logger.error("Error saving updated file", e);
            throw new VirtualViewerAPIException("Error saving updated file", e);
        }
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
        String clientInstanceId = input.getClientInstanceId();
        String documentKey = input.getDocumentId();
        String annotationKey = input.getAnnotationId();
        logger.trace("getAnnotationContent()");
        String annotationFilename = documentKey + "." + annotationKey + ".ann";
        String fullFilePath = gFilePath + annotationFilename;
        logger.trace("Retrieving annotation file: {}", fullFilePath);
        Map props = null;
        ContentHandlerResult propsResult = getAnnotationProperties(input);
        
        if (propsResult != null) {
            props = propsResult.getAnnotationProperties();
            
            //Content handler can retrieve permission level and redaction flag
            Boolean redactionFlag = (Boolean) props.get(AnnotationLayer.PROPERTIES_KEY_REDACTION_FLAG);
            Integer permissionLevel = (Integer) props.get(AnnotationLayer.PROPERTIES_KEY_PERMISSION_LEVEL);            
        }
        
        try {
            File file = new File(fullFilePath);
            byte[] bytes = ClientServerIO.getFileBytes(file);
            ContentHandlerResult result = new ContentHandlerResult();
            result.put(ContentHandlerResult.KEY_ANNOTATION_CONTENT, bytes);
            result.put(ContentHandlerResult.KEY_ANNOTATION_DISPLAY_NAME, input.getAnnotationId());
            result.put(ContentHandlerResult.KEY_ANNOTATION_PROPERTIES, props);
            
            return result;
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * @see com.snowbound.contenthandler.interfaces.AnnotationsInterface#getAnnotationProperties(ContentHandlerInput)
     */
    @Override
    public ContentHandlerResult getAnnotationProperties(ContentHandlerInput input)
            throws VirtualViewerAPIException {
        HttpServletRequest request = input.getHttpServletRequest();
        String clientInstanceId = input.getClientInstanceId();
        String documentKey = input.getDocumentId();
        String annotationKey = input.getAnnotationId();
        logger.trace("getAnnotationProperties()");
        Hashtable properties = new Hashtable();
        String baseAnnFilename = documentKey + "." + annotationKey;
        String annFilename = gFilePath + baseAnnFilename + ".ann";
        
        // Is it a regular annotation layer ?
        File file = new File(annFilename);
        if (file.exists()) {
            properties.put(AnnotationLayer.PROPERTIES_KEY_PERMISSION_LEVEL, PermissionLevel.DELETE);
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
        String documentKey = input.getDocumentId();
        logger.trace("GetBookmarkContent: clientInstanceId {}", clientInstanceId);
        String bookmarkFilename = documentKey + ".bookmarks.xml";
        String fullFilePath = gFilePath + bookmarkFilename;
        logger.trace("Retrieving bookmark file: {}", fullFilePath);
        try {
            File file = new File(fullFilePath);
            byte[] bytes = ClientServerIO.getFileBytes(file);
            ContentHandlerResult result = new ContentHandlerResult();
            result.put(ContentHandlerResult.KEY_BOOKMARK_CONTENT, bytes);
            return result;
        } catch (IOException e) {
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
        String documentId = input.getDocumentId();

        return saveBookmarkContent(clientInstanceId, documentId, null);
    }

    private ContentHandlerResult saveBookmarkContent(String clientInstanceId, String documentId, byte[] data) {
        logger.trace("saveBookmarkContent...{} clientInstanceId: {}", documentId, clientInstanceId);
        String fullFilePath = gFilePath + documentId + ".bookmarks.xml";
        File file = new File(fullFilePath);
        if (data == null) {
            if (file.exists()) {
                file.delete();
            }
            return ContentHandlerResult.VOID;
        }
        try {
            ClientServerIO.saveFileBytes(data, file);
        } catch (Exception e) {
            logger.error("Error while saving bookmark content to file", e);
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
        String documentKey = input.getDocumentId();
        logger.trace("getWatermarkContent: clientInstanceId {}", clientInstanceId);
        String watermarkFilename = documentKey + ".watermarks.json";
        String fullFilePath = gFilePath + watermarkFilename;
        logger.trace("Retrieving watermark file: {}", fullFilePath);
        try {
            File file = new File(fullFilePath);
            byte[] bytes = ClientServerIO.getFileBytes(file);
            ContentHandlerResult result = new ContentHandlerResult();
            result.put(ContentHandlerResult.KEY_WATERMARK_CONTENT, bytes);
            return result;
        } catch (IOException e) {
            return null;
        }
    }  
    
    /**
     * @throws VirtualViewerAPIException can throw VirtualViewer exception to raise and log an error 
     * @see com.snowbound.contenthandler.interfaces.WatermarksInterface#deleteWatermarkContent(ContentHandlerInput)
     */
    @Override
    public ContentHandlerResult deleteWatermarkContent(ContentHandlerInput input) 
        throws VirtualViewerAPIException {
        String clientInstanceId = input.getClientInstanceId();
        String documentId = input.getDocumentId();
        
        return saveWatermarkContent(clientInstanceId, documentId, null);
    }

    /**
     *
     * @param clientInstanceId
     * @param documentId
     * @param data
     * @return
     */
    private ContentHandlerResult saveWatermarkContent(String clientInstanceId, String documentId, byte[] data) {
        logger.trace("saveWatermarkContent...{} clientInstanceId: {}", documentId, clientInstanceId);
        String fullFilePath = gFilePath + documentId + ".watermarks.json";
        File file = new File(fullFilePath);
        if (data == null) {
            if (file.exists()) {
                file.delete();
            } else {
                return ContentHandlerResult.VOID;
            }
        }
        try {
            ClientServerIO.saveFileBytes(data, file);
        } catch (Exception e) {            
            logger.error("Error while saving watermark content to file", e);
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
        String documentKey = input.getDocumentId();
        logger.trace("getNotesContent: clientInstanceId {}", clientInstanceId);
        String bookmarkFilename = documentKey + ".notes.xml";
        String fullFilePath = gFilePath + bookmarkFilename;
        logger.trace("Retrieving notes file: {}", fullFilePath);
        try {
            File file = new File(fullFilePath);
            byte[] bytes = ClientServerIO.getFileBytes(file);
            ContentHandlerResult result = new ContentHandlerResult();
            result.put(ContentHandlerResult.KEY_NOTES_CONTENT, bytes);
            return result;
        } catch (IOException e) {
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
        String documentId = input.getDocumentId();
        
        return saveNotesContent(clientInstanceId, documentId, null);
    }

    /**
     *
     * @param clientInstanceId
     * @param documentId
     * @param data
     * @return
     */
    private ContentHandlerResult saveNotesContent(String clientInstanceId, String documentId, byte[] data) {
        logger.trace("saveNotesContent...{} clientInstanceId: {}", documentId, clientInstanceId);
        String fullFilePath = gFilePath + documentId + ".notes.xml";
        File file = new File(fullFilePath);
        if (data == null) {
            if (file.exists()) {
                file.delete();
            }

            return ContentHandlerResult.VOID;
        }

        try {
            ClientServerIO.saveFileBytes(data, file);
        } catch (Exception e) {
            logger.error("Error while saving note content to file", e);
        }
        return new ContentHandlerResult();
    }

    /**
     * @see com.snowbound.contenthandler.interfaces.VirtualViewerContentHandlerInterface#getDocumentContent(ContentHandlerInput)
     */
    @Override
    public ContentHandlerResult getDocumentContent(ContentHandlerInput input)
            throws VirtualViewerAPIException {
        String clientInstanceId = input.getClientInstanceId();
        String key = input.getDocumentId();

        ContentHandlerResult result = new ContentHandlerResult();

        // Following is an example of how to pass back external references: the requested document may link to other
        // documents stored in the content handler. If those documents are passed back to VirtualViewer, VirtualViewer
        // will composite them into a single image. Note that this very naive implementation is not intended for production use.
        // External references are currently only supported for CAD/DWG documents.
        Boolean returnExternalReferences = false;
        String externalReferenceParentFileName = null;
        if (key.startsWith("IncludesExternalReferences:")) {
            externalReferenceParentFileName = key.split(":")[1];
            returnExternalReferences = true;
        }
        if (key.toLowerCase().endsWith(".dwg") ||
                 key.toLowerCase().endsWith(".dxf") ||
                 key.toLowerCase().endsWith(".dgn")) {
            returnExternalReferences = true;
            externalReferenceParentFileName = key;
        }

        if (returnExternalReferences) {
            File parentDir = new File(gFilePath);
            if (!parentDir.exists() || !parentDir.isDirectory()) {
                throw new VirtualViewerAPIException("Document not found: " + ClientServerIO.makeXssSafe(key));
            }

            List<ExternalReference> vXrefs = new ArrayList<>();

            String[] siblingFiles = parentDir.list();

            for (String siblingFilename : siblingFiles) {
                ExternalReference xref = new ExternalReference();
                byte[] xrefData = null;
                String xrefName = siblingFilename;
                File siblingFile = new File(parentDir, xrefName);
                if (!xrefName.equals(externalReferenceParentFileName) &&
                    (xrefName.toLowerCase().endsWith(".dwg"))) {
                    try {
                        xrefData = ClientServerIO.getFileBytes(siblingFile);
                    } catch (IOException e) {                        
                        logger.error("Error while retrieving xref data from file", e);
                    }
                    xref.setExternalReferenceData(xrefData);
                    xref.setExternalReferenceID(xrefName);
                    vXrefs.add(xref);
                }
            }
            result.put(ContentHandlerResult.KEY_EXTERNAL_REFERENCE_CONTENT_ELEMENTS, vXrefs);
            result.put(ContentHandlerResult.KEY_RETRIEVED_EXTERNAL_REFERENCES, true);
        }

        // This is an example of how to use VV's "Sparse Document" mechanism.  It is not intended for production use.
        // The prefix SparseDocument: is used for testing; it is not required in the ID for Sparse Documents.
        if (key.startsWith(PREFIX_SPARSE_DOCUMENT)) {
            List<byte[]> vectorOfFiles = new ArrayList<>();

            int pageNumber = input.getSparseRequestedPageNumber();
            int pageCount = input.getSparseRequestedPageCount();

            String dirName = key.split(":")[1];

            // create a file that is really a directory
            String basePath = gFilePath + "/" + dirName;
            File dir = new File(basePath);

            if (!dir.exists() || !dir.isDirectory()) {
                throw new VirtualViewerAPIException("Document not found: " +
                         ClientServerIO.makeXssSafe(key));
            }

            // get a listing of all files in the directory
            String[] filesInDir = dir.list();

            int startIndex = pageNumber;
            int endIndex = pageNumber + pageCount;
            if (endIndex > filesInDir.length || pageCount == 0) {
                endIndex = filesInDir.length;
            }

            for (int x = startIndex; x < endIndex; x++) {
                String path = basePath + "/" + filesInDir[x];
                File file = new File(path);
                try {
                    byte[] data = ClientServerIO.getFileBytes(file);
                    vectorOfFiles.add(data);
                } catch (IOException e) {                    
                    logger.error("Error while retrieving document content from file", e);
                }

            }

            result.put(ContentHandlerResult.KEY_DOCUMENT_SPARSE_ELEMENTS, vectorOfFiles);
            result.put(ContentHandlerResult.KEY_DOCUMENT_SPARSE_PAGE_INDEX, pageNumber);
            result.put(ContentHandlerResult.KEY_DOCUMENT_SPARSE_RETURN_PAGE_COUNT, vectorOfFiles.size());
            result.put(ContentHandlerResult.KEY_DOCUMENT_SPARSE_TOTAL_PAGE_COUNT, filesInDir.length);

            result.put(ContentHandlerResult.KEY_DOCUMENT_DISPLAY_NAME, dirName);

            if (contentHandlerDebug) {
                String reversed = new StringBuilder(key).reverse().toString().toUpperCase();

                result.put(ContentHandlerResult.KEY_DOCUMENT_DISPLAY_NAME, reversed);
            }
        } // This is an example of how to use VV's Content Elements mechanism.  It is not intended for production use.
        // The prefix CompountDocument: is used for testing; it is not required in the ID for Compound Documents.
        else if (key.startsWith(PREFIX_COMPOUND_DOCUMENT)) {
            List<byte[]> vectorOfFiles = new ArrayList<>();

            String documentDefinition = key.substring(key.indexOf(':') + 1);
            StringTokenizer st = new StringTokenizer(documentDefinition, ",");

            while (st.hasMoreTokens()) {
                String documentComponent = st.nextToken();

                String fullFilePath = gFilePath + documentComponent;
                File file = new File(fullFilePath);
                byte[] fileBytes;

                try {
                    fileBytes = ClientServerIO.getFileBytes(file);
                } catch (FileNotFoundException fnfe) {
                    /* Removing stack trace here, as it was unnecessary */
                    logger.error("Document not found", fnfe);
                    throw new VirtualViewerAPIException("Document not found: " + ClientServerIO.makeXssSafe(key), fnfe);
                } catch (IOException e) {
                    logger.error("Could not read document file", e);
                    return null;
                }

                vectorOfFiles.add(fileBytes);
            }

            result.put(ContentHandlerResult.KEY_DOCUMENT_CONTENT_ELEMENTS, vectorOfFiles);
            result.put(ContentHandlerResult.KEY_DOCUMENT_DISPLAY_NAME, documentDefinition);

            if (contentHandlerDebug) {
                String reversed = new StringBuilder(key).reverse().toString().toUpperCase();

                result.put(ContentHandlerResult.KEY_DOCUMENT_DISPLAY_NAME, reversed);
            }
        } else {
            logger.trace("getDocumentContent: Retrieving single document");

            String fullFilePath = gFilePath + key;
            File file = new File(fullFilePath);

            try {
                result.put(ContentHandlerResult.KEY_DOCUMENT_CONTENT, ClientServerIO.getFileBytes(file));

                if (contentHandlerDebug) {
                    String reversed = new StringBuilder(key).reverse().toString();

                    result.put(ContentHandlerResult.KEY_DOCUMENT_DISPLAY_NAME, reversed);
                }
            } catch (FileNotFoundException fnfe) {
                /* Removing stack trace here, as it was unnecessary */
                logger.error("Document not found", fnfe);
                throw new VirtualViewerAPIException("Document not found: " + ClientServerIO.makeXssSafe(key), fnfe);
            } catch (IOException e) {
                logger.error("Could not read document file", e);
                return null;
            }
        }

        return result;
    }

    public ContentHandlerResult getDocumentContentByFile(ContentHandlerInput input)
            throws VirtualViewerAPIException {
        String clientInstanceId = input.getClientInstanceId();
        String key = input.getDocumentId();
        logger.trace("FileContentHandler.getDocumentContent({})", key);
        String fullFilePath = null;
        try {
            fullFilePath = gFilePath + URLDecoder.decode(key, StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException e) {
            logger.error("Unsupported encoding in document key", e);
            return null;
        }
        File file = new File(fullFilePath);
        ContentHandlerResult result = new ContentHandlerResult();            
        result.put(ContentHandlerResult.KEY_DOCUMENT_FILE, file);
        result.put(ContentHandlerResult.KEY_DOCUMENT_DISPLAY_NAME, key);
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

        String clientInstanceId = input.getClientInstanceId();
        String documentKey = input.getDocumentId();
        String annotationKey = input.getAnnotationId();
        String annotationFilename = documentKey + "." + annotationKey + ".ann";
        String fullFilePath = gFilePath + annotationFilename;
        logger.trace("Deleting annotation file: {}", fullFilePath);
        try {
            File file = new File(fullFilePath);
            file.delete();
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
     * @throws com.snowbound.contenthandler.VirtualViewerAPIException can throw VirtualViewer exception to raise and log an error
     * @see com.snowbound.contenthandler.interfaces.AvailableDocumentsInterface#getAvailableDocumentIds(ContentHandlerInput)
     */
    @Override
    public ContentHandlerResult getAvailableDocumentIds(ContentHandlerInput input)
            throws VirtualViewerAPIException {
        List<String> validExtensions = VirtualViewerFormatHash.getInstance().getKnownExtensions();        
                
        String clientInstanceId = input.getClientInstanceId();
        File imgDirectory = new File(gFilePath);
        String[] listArray = imgDirectory.list(new FileContentHandlerFileNameFilter());
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
     * @see com.snowbound.contenthandler.interfaces.SendDocumentInterface#sendDocumentContent(ContentHandlerInput)
     */
    @Override
    public ContentHandlerResult sendDocumentContent(ContentHandlerInput input)
            throws VirtualViewerAPIException {
        if (readOnlyMode) {
            throw new VirtualViewerAPIException(READ_ONLY_ERROR_MESSAGE);
        }

        ContentHandlerResult retVal = new ContentHandlerResult();
        HttpServletRequest request = input.getHttpServletRequest();
        String clientInstanceId = input.getClientInstanceId();
        String documentKey = input.getDocumentId();
        int outputFormat = input.getDocumentFormat();
        String extension = VirtualViewerFormatHash.getInstance().getFormat(outputFormat).getExtension();
        boolean mergeAnnotations = input.mergeAnnotations();
        byte[] data = input.getDocumentContent();
        File saveFile = new File(gFilePath + "sendDocument-" + documentKey + "." + extension);
        ClientServerIO.saveFileBytes(data, saveFile);
        return retVal;
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
                        logger.trace("Key: {}, value: {}", key, value);
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

        Map<String, AnnotationLayer> annotationHash = new HashMap<>();

        String documentId = input.getDocumentId();
        String clientInstanceId = input.getClientInstanceId();

        ContentHandlerResult annResult = this.getAnnotationNames(input);
        String[] annNames = annResult.getAnnotationNames();

        if (annNames != null) {
            for (String annotationId : annNames) {
                ContentHandlerInput cInput = new ContentHandlerInput(documentId, clientInstanceId);

                cInput.setAnnotationId(annotationId);

                ContentHandlerResult cResult = this.getAnnotationContent(cInput);

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
        String documentKey = input.getDocumentId();
        logger.trace("getOCRDataForDocument: clientInstanceId {}", clientInstanceId);
        String ocrDataFilename = documentKey + ".ocr-text.json";
        String fullFilePath = gFilePath + ocrDataFilename;
        
        try {
            File file = new File(fullFilePath);
            byte[] bytes = ClientServerIO.getFileBytes(file);
            ContentHandlerResult result = new ContentHandlerResult();
            result.put(ContentHandlerResult.KEY_OCR_DATA_JSON, bytes);
            logger.trace("Retrieving OCR data file: {}", ocrDataFilename);
            return result;
        } catch (IOException e) {
            logger.trace("No OCR data found for: {}", documentKey);
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
