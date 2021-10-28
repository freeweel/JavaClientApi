package marklogic;

import common.StreamWriter;

import java.io.InputStream;

public class MarkLogicStreamWriter implements StreamWriter {
    private final MarkLogicDataMovementUtil dmsdk;

    public MarkLogicStreamWriter(MarkLogicDataMovementUtil dmsdk) {
        this.dmsdk = dmsdk;
    }

    @Override
    public void write(String path, InputStream inputStream) throws Exception {
        dmsdk.addDocument(path, inputStream);
    }
}
