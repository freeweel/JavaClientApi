package config;

import java.io.File;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

/**
 * Configuration object for AWS and MarkLogic credentials
 */
public class Config {
	private DocumentContext json;
	private File configFile;
	public final String AWS_KEY, AWS_SECRET, AWS_REGION, ML_USER, ML_PASSWORD, ML_HOST;
	public final int ML_STAGING_PORT, ML_FINAL_PORT;

	// Private constructor
	private Config(File configFile) throws Exception {
		this.configFile = configFile;
		this.json = JsonPath.parse(configFile);

		AWS_KEY = (String)this.get("aws_key");
		AWS_SECRET= (String)this.get("aws_secret");
		AWS_REGION = (String)this.get("aws_region");
		ML_USER = (String)this.get("ml_user");
		ML_PASSWORD = (String)this.get("ml_password");
		ML_HOST = (String)this.get("ml_host");
		ML_STAGING_PORT = (Integer)this.get("ml_staging_db_port");
		ML_FINAL_PORT = (Integer)this.get("ml_final_db_port");
	}

	/**
	 * Constructor
	 * @param configFile A JSON config file
	 * @return A Config object
	 * @throws Exception
	 */
	public static Config setFile(File configFile) throws Exception {
		// Look for hidden file
		if (!configFile.exists()) {
			throw new Exception("Access keys expected in file " + configFile.getCanonicalPath());
		}
		return new Config(configFile);
	}

	// Internal method to read JSON (allows for additional error checking in the future)
	private Object get(String key) { 
		return this.json.read(key);
	}

	// Get the config file
	public File getConfigFile() { return this.configFile; }
}
