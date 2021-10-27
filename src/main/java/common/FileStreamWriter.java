package common;

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;

/**
 * Save the contents read in to an InputStream out to a file
 * NOTE: The stream contents are never stored in memory
 *
 * Mainly built as a local test, but this could be useful to download a significant
 * amount of documents from MarkLogic to a File System.
 */
public class FileStreamWriter implements StreamWriter {
    
    @Override
    /**
     * Write the contents read to an InputStream to a File
     * @param path The full path to the file (e.g., '/test/out/file.pdf')
     * @param inputStream An input stream that has been read
     */
    public void write(String path, InputStream inputStream) throws Exception {
        FileOutputStream fileOutputStream = null;
        try {
            File outFile = new File(path);
            outFile.getParentFile().mkdirs();
            fileOutputStream = new FileOutputStream(outFile);
            IOUtils.copy(inputStream, fileOutputStream);
        }
        finally {
            inputStream.close();
            fileOutputStream.close();
        }
    }

}
