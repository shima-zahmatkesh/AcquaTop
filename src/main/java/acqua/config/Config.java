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
	
	public Integer getUpdateBudget(){
		return config.getInt("query.updatebudget");
	} 
	
	public Long getQueryStartingTime(){
		return config.getLong("query.start");
	}

	public Integer getQueryWindowSlide(){
		return config.getInt("query.window.slide");
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
	
	public String getProjectPath(){
		return config.getString("filesystem.path");
	}
	
	public String getDatasetFolder(){
		return config.getString("dataset.folder");
	}
	
	public String getDatasetDb(){
		return config.getString("dataset.db");
	}
	
	public Long getQueryFilterThreshold(){
		return config.getLong("query.filter.threshold");
	}
	
	public void setDatasetDb (String dbName){
		config.setProperty("dataset.db", dbName);
	}
	
	public Integer getExperimentIterationNumber(){
		return config.getInt("experiment.iteration.number");
	}
	
	public Boolean getQueryWithFiltering(){
		if(config.getString("query.with.filtering").equals("true"))
			return true;
		return false;
	}
	
	public Boolean getConsideringFilteringInUpdatePolicy(){
		if(config.getString("considering.filtering.in.update.policy").equals("true"))
			return true;
		return false;
	}
	public Long getQueryDifferenceThreshold(){
		return config.getLong("difference.threshold");
	}
	
	public void setUpdateBudget(String b){
		config.setProperty("query.updatebudget", b);
	} 
	
	public Integer getDatabaseNumber(){
		return config.getInt("database.number");
	}

}
