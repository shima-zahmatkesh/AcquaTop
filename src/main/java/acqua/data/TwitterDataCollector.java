package acqua.data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import acqua.config.Config;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterDataCollector {
	public static void main(String[] args) {
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		.setOAuthConsumerKey(Config.INSTANCE.getTwitterConsumerKey())
		.setOAuthConsumerSecret(Config.INSTANCE.getTwitterConsumerSecret())
		.setOAuthAccessToken(Config.INSTANCE.getTwitterAccessToken())
		.setOAuthAccessTokenSecret(Config.INSTANCE.getTwitterAccessTokenSecret());
		TwitterFactory tf = new TwitterFactory(cb.build());
		Twitter twitter = tf.getInstance();

		Connection conn = null;
		try {
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:verified.db");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

//		initDb(conn);
		long cursor = 1488689511772407664l;
		while(cursor!=0){
			System.out.println("next cursor:" + cursor);
			cursor = grabUserIds(twitter, conn, cursor);
		}
	}
	public static void initDb(Connection conn){
		try {
			Statement stmt = conn.createStatement();
			//stmt.executeQuery("DROP INDEX IF EXISTS timeIndex ON BKG;");
			stmt.executeUpdate(" DROP TABLE IF EXISTS verified ;");
			String sql = "CREATE TABLE  `verified` ( " +
					" `id`           BIGINT    NOT NULL, " + 
					" `screenname`           TEXT, " + 
					" `followers`           INT, " + 
					" `followings`           INT, " + 
					" `description`           TEXT); ";
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static long grabUserIds(Twitter twitter, Connection conn, long cursor){
		try {
			PreparedStatement query = conn.prepareStatement("INSERT INTO `verified` (`id`) VALUES (?)");
			IDs ids = twitter.getFollowersIDs("verified", cursor);
			for(long id : ids.getIDs()){
				query.setLong(1, id);
				query.addBatch();
			}
			query.executeBatch();
			return ids.getNextCursor();
		} catch (TwitterException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return -1;
	}
	
	public static int getMax(Connection conn){
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM `verified`");
			rs.next();
			return rs.getInt(1);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return -1;

	}

}
