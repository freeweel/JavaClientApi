package config;

import java.io.File;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration object for AWS and MarkLogic credentials
 */
public class Config {
	private final DocumentContext json;
	private final File configFile;
	public final String AWS_KEY, AWS_SECRET, AWS_REGION, AWS_BUCKET, AWS_PREFIX;
	public final String ML_USER, ML_PASSWORD, ML_HOST;
	public final int ML_STAGING_PORT, ML_FINAL_PORT;
	private static final Logger LOGGER = LoggerFactory.getLogger("Log");

	// Private constructor
	private Config(File configFile) throws Exception {
		this.configFile = configFile;
		this.json = JsonPath.parse(configFile);

		try {
			AWS_KEY = (String) this.get("aws_key");
			AWS_SECRET = (String) this.get("aws_secret");
			AWS_REGION = (String) this.get("aws_region");
			AWS_BUCKET = (String) this.get("aws_bucket");
			AWS_PREFIX = (String) this.get("aws_prefix");
			ML_USER = (String) this.get("ml_user");
			ML_PASSWORD = (String) this.get("ml_password");
			ML_HOST = (String) this.get("ml_host");
			ML_STAGING_PORT = (Integer) this.get("ml_staging_db_port");
			ML_FINAL_PORT = (Integer) this.get("ml_final_db_port");
		}
		catch(Exception e) {
			LOGGER.error("Unable to read configuration file.");
			throw(e);
		}
	}

	/**
	 * Constructor
	 * @param configFile A JSON config file
	 * @return A Config object
	 * @throws Exception on error
	 */
	public static Config setFile(File configFile) throws Exception {
		// Look for hidden file
		if (!configFile.exists()) {
			throw new Exception("Access keys expected in file " + configFile.getCanonicalPath());
		}
		return new Config(configFile);
	}

	// Internal method to read JSON (allows for additional error checking)
	private Object get(String key) throws Exception {
		try {
			String param = this.json.read(key);
			if (param == null || param.isEmpty()) throw new Exception(key);
			return param;
		}
		catch(Exception e) {
			LOGGER.error("Unable to initialize config parameter " + key);
			throw(e);
		}
	}

	// Get the config file
	public File getConfigFile() { return this.configFile; }
}
