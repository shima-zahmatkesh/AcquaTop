package acqua.data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;

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

			conn.setAutoCommit(true); // only required if autocommit state not known
			Statement stat = conn.createStatement(); 
			stat.executeUpdate("PRAGMA synchronous = OFF;");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


		int i=0;

//		initDb(conn);
//
//		long cursor = -1;
//		try {
//			while(cursor!=0){
//				i++;
//				System.out.println("next cursor:" + cursor);
//				cursor = grabUserIds(twitter, conn, cursor);
//				if(i==15){
//					Thread.sleep(1000*15*60);
//					i=0;
//				}
//				Thread.sleep(1000);
//			}
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}

		long updatedUsers = -1;
		i=0;
		try {
			while(updatedUsers!=0){
				i++;
				updatedUsers = grabUserData(twitter, conn);
				if(i==60){
					Thread.sleep(1000*15*60);
					i=0;
				}
				Thread.sleep(1000);
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
					" `name`           TEXT, " + 
					" `followers`           INT, " + 
					" `followings`           INT, " + 
					" `status`           INT, " + 
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
			IDs ids = twitter.getFriendsIDs("verified", cursor);
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

	public static int grabUserData(Twitter twitter, Connection conn){
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT id FROM `verified` WHERE followers IS NULL LIMIT 100");
			long[] ids = new long[100];
			int i=0;
			while(rs.next()!=false){
				ids[i++]=rs.getLong(1);
			}
			rs.close();
			stmt.close();

			PreparedStatement query = conn.prepareStatement(
					"UPDATE `verified` " +
					"SET `screenname`=?, " + 
					"`name`=?, "+ 
					"`followers`=?, "+ 
					"`followings`=?, "+ 
					"`status`=?, "+ 
					"`description`=? "+
					"WHERE `id`=?");

			ResponseList<User> users = twitter.lookupUsers(ids);

			Iterator<User> it = users.iterator();

			while(it.hasNext()){
				User user = it.next();
				query.setString(1, user.getScreenName());
				query.setString(2, user.getName());
				query.setLong(3, user.getFollowersCount());
				query.setLong(4, user.getFriendsCount());
				query.setLong(5, user.getStatusesCount());
				query.setString(6, user.getDescription());
				query.setLong(7, user.getId());
				query.addBatch();
			}
			query.executeBatch();
			query.closeOnCompletion();
			return ids.length;
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
