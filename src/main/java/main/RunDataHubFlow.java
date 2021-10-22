package main;

import config.Config;
import marklogic.MarkLogicDataHub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class RunDataHubFlow {
    final private static File configFile = new File("src/main/resources/AWSAccessKeys.secret");
    private static Logger LOGGER = LoggerFactory.getLogger("Log");

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Config config = Config.getConfig();
        MarkLogicDataHub dataHub = new MarkLogicDataHub();
        dataHub.runFlow("Person-Flow", "2");
    }
}
