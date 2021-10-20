package config;

public class Config {
	public enum ENV {local, dev};
	private int item;
	
	private String[] server = {"localhost","wps-mlogic-t01"};
	private int[] portStaging = {8010,8020};
	private int[] portFinal = {8011,8021};
	private String[] user = {"admin","0024900"};
	private String[] pwd = {"admin","Kia20.optima!AT"};
	
	
	public Config(ENV env) {
		this.item = getEnv(env);
	}
	
	public static Config getConfig(ENV env) {
		return new Config(env);
	}
	
	private static int getEnv(ENV env) {
		if (env == ENV.local) return 0;
		else if (env == ENV.dev) return 1; 
		return 0;
	}
	
	public String server() {
		return this.server[item];
	}
	
	public int portStaging() {
		return this.portStaging[item];
	}

	public int portFinal() {
		return this.portFinal[item];
	}

	public String user() {
		return this.user[item];
	}
	
	public String pwd() {
		return this.pwd[item];
	}
}
