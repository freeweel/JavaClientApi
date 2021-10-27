package common;

import java.io.InputStream;

/**
 * This interface is used to write a document to a repository using a path name
 * to identify the object in the repository.
 *
 * Examples are writing to File, AWS S3 Bucket, or MarkLogic directory
 *
 * NOTE: May be expanded for more system dependent options in the future
 *       Examples are MarkLogic permissions, collections, metadata, etc. and S3 ObjectMetadata
 */
public interface StreamWriter {
    /**
     * Write the document from a Stream
     * @param path The full path to the document
     * @param inputStream An Input or Output Stream (depends on specific implementation)
     * @throws Exception
     */
    public void write(String path, InputStream inputStream) throws Exception;
}
