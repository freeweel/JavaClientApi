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

	// Private constructor
	private Config(File configFile) throws Exception {
		this.configFile = configFile;
		this.json = JsonPath.parse(configFile);
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

	// Get config properties for AWS
	public String getAwsKey() { return (String)get("aws_key"); }
	public String getAwsSecret() { return (String) get("aws_secret"); }
	public String getAwsRegion() { return (String) get("aws_region"); }

	// Get config properties for MarkLogic
	public String getMLHost() { return (String) get("ml_host"); }
	public String getMLUser() { return (String) get("ml_user"); }
	public String getMLPassword() { return (String) get("ml_password"); }
	public int getMLStagingDbPort() { return (Integer) get("ml_staging_db_port"); }
	public int getMLFinalDbPort() { return (Integer) get("ml_final_db_port"); }
}
