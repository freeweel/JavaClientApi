package marklogic;

import common.StreamWriter;

import java.io.InputStream;

public class MarkLogicStreamWriter implements StreamWriter {
    private final MarkLogicDataMovement dmsdk;

    public MarkLogicStreamWriter(MarkLogicDataMovement dmsdk) {
        this.dmsdk = dmsdk;
    }

    @Override
    public void write(String path, InputStream inputStream) throws Exception {
        dmsdk.addDocument(path, inputStream);
    }
}
