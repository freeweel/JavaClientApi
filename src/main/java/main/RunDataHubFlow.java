package main;

import marklogic.MarkLogicDataHubUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RunDataHubFlow {
    @SuppressWarnings("unused")
    private static final Logger LOGGER = LoggerFactory.getLogger("Log");

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        MarkLogicDataHubUtil dataHub = new MarkLogicDataHubUtil();
        dataHub.runFlow("Person-Flow", "2");
    }
}
