package acqua.config;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Config {
	public static final Config INSTANCE = new Config();
	private static final Logger logger = LoggerFactory.getLogger(Config.class); 
	
	private Configuration config;
	
	private Config(){
		try {
			config = new PropertiesConfiguration("acqua.properties");
		} catch (ConfigurationException e) {
			logger.error("Error while reading the configuration file", e);
		}
	}
	
	public Long getQueryStartingTime(){
		return config.getLong("query.start");
	}

	public Integer getQueryWindowWidth(){
		return config.getInt("query.window.width");
	}
	
	public String getTwitterConsumerKey(){
		return config.getString("twitter.consumer.key");
	}
	
	public String getTwitterConsumerSecret(){
		return config.getString("twitter.consumer.secret");
	}

	public String getTwitterAccessTokenSecret(){
		return config.getString("twitter.accesstoken.secret");
	}
	
	public String getTwitterAccessToken(){
		return config.getString("twitter.accesstoken");
	}

}
