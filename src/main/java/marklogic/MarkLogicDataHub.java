package marklogic;

import com.marklogic.hub.DatabaseKind;
import com.marklogic.hub.flow.FlowInputs;
import com.marklogic.hub.flow.FlowRunner;
import com.marklogic.hub.flow.RunFlowResponse;
import com.marklogic.hub.flow.impl.FlowRunnerImpl;
import com.marklogic.hub.impl.HubConfigImpl;
import com.marklogic.mgmt.util.SimplePropertySource;
import common.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

/**
 * Provides a method to run a MarkLogic Data Hub flow
 */
public class MarkLogicDataHub {
    private final Config config;
    private static Logger LOGGER = LoggerFactory.getLogger("Log");

    public MarkLogicDataHub() throws Exception {
        this.config = Config.getConfig();
    }

    /**
     * @param flowName The name of the flow to run
     * @throws Exception
     */
    public void runFlow(String flowName, String... steps) throws Exception {
        // Instantiate a HubConfig with DHF's default set of properties, and then start customizing it
        HubConfigImpl hubConfig = new HubConfigImpl(config.ML_HOST, config.ML_USER, config.ML_PASSWORD);

        // Customization for non-standard inputs (can be found in gradle.properties)
        hubConfig.setAuthMethod(DatabaseKind.STAGING, "digest");
        hubConfig.setAuthMethod(DatabaseKind.FINAL, "digest");
        hubConfig.setAuthMethod(DatabaseKind.JOB, "digest");
        hubConfig.setPort(DatabaseKind.STAGING, config.ML_STAGING_PORT);
        hubConfig.setPort(DatabaseKind.FINAL, config.ML_FINAL_PORT);
        hubConfig.setPort(DatabaseKind.JOB, config.ML_JOBS_PORT);
        hubConfig.setSimpleSsl(DatabaseKind.STAGING, false);
        hubConfig.setSimpleSsl(DatabaseKind.FINAL, false);
        hubConfig.setSimpleSsl(DatabaseKind.JOB, false);
        Properties props = new Properties();
        props.setProperty("hubSsl", "false");
        hubConfig.applyProperties(new SimplePropertySource(props));

        FlowRunner flowRunner = new FlowRunnerImpl(hubConfig.newHubClient());
        FlowInputs flowInputs = new FlowInputs("Person-Flow");

        // To run only a subset of the steps in the flow, uncomment the following line and specify the sequence numbers of the steps to run.
        if (steps.length > 0) {
            flowInputs.setSteps(Arrays.asList(steps));
        }
        flowInputs.setStepConfig(new HashMap<String, Object>());

        // Set file path for ingestion steps (We don't expect to have ingestion steps since that will be done from S3)
        // inputs.setInputFilePath("<path>");

        // Run the flow.
        RunFlowResponse response = flowRunner.runFlow(flowInputs);

        // Wait for the flow to end.
        flowRunner.awaitCompletion();

        // Display the response.
        LOGGER.info("Response: " + response);
    }
}

