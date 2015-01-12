package acqua.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import acqua.config.Config;

import twitter4j.ResponseList;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.User;
import twitter4j.conf.ConfigurationBuilder;


public class TwitterFollowerCollector {
	private ConfigurationBuilder cb;
	HashMap<Long,HashMap<Long,Integer>> snapshots;//timeStamp, list<userId, follower>
	/*public TwitterFollowerCollector(){}//constructor only for querying*/
	//this constructor is for listening to twitter stream
	public TwitterFollowerCollector(){
		cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		.setOAuthConsumerKey(Config.INSTANCE.getTwitterConsumerKey())
		.setOAuthConsumerSecret(Config.INSTANCE.getTwitterConsumerSecret())
		.setOAuthAccessToken(Config.INSTANCE.getTwitterAccessToken())
		.setOAuthAccessTokenSecret(Config.INSTANCE.getTwitterAccessTokenSecret());
		snapshots = new HashMap<Long,HashMap<Long,Integer>>();
	}
	public HashMap<Long, String> readIntialUserSet(String userListFilePath){
		HashMap<Long, String> result = new HashMap<Long, String>();
		InputStream    fis;
		BufferedReader br;
		try{
			fis = new FileInputStream(userListFilePath);//"D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/followers.csv");
			br = new BufferedReader(new InputStreamReader(fis));

			String br1 = br.readLine();
			String[] idStr = br1.split(",");
			String br2=br.readLine();
			String[] idStr2 = br2.split(",");
			final long[] monitoredIds = new long[idStr.length];
			for(int o=0;o<idStr.length;o++){
				//monitoredIds[o]=Long.parseLong(idStr[o]);
				result.put(Long.parseLong(idStr[o]), idStr2[o]);
			}
			br.close();
		}catch(Exception e){
			e.printStackTrace();
		}

		br = null;
		fis = null;
		return result;
	}
	public void captureSnapshots(String userListFilePath, String snapshotOutputPath){		
		TwitterFactory tf = new TwitterFactory(cb.build());
		Twitter twitter = tf.getInstance();
		try{
			FileWriter followerFile=new FileWriter(new File(snapshotOutputPath));//"D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/followerSnapshotsFile2.txt"));

			Set<Long> initialUserFollowerSet= readIntialUserSet(userListFilePath).keySet();

			if(initialUserFollowerSet.size()>400){
				System.out.println("too many users! i exit");
				System.exit(0);
			}

			long bins = Math.round(initialUserFollowerSet.size()/100+.49);

			long[][] monitoredIds=new long[4][];
			Iterator<Long> it = initialUserFollowerSet.iterator();
			while(it.hasNext()){
				for(int i=0;i<bins;i++){
					monitoredIds[i]=new long[100];
					for(int j=0; j<100; j++){
						monitoredIds[i][j]=it.next();
					}
				}
			}
			for(int y=0;y<120;y++){
				for(int i=0; i<bins; i++){
					ResponseList<User> users = twitter.lookupUsers(monitoredIds[i]);
					for(User u : users){				
						followerFile.write(u.getId()+ ",\"" +u.getScreenName()+"\","+ u.getName()+ "," +u.getFollowersCount()+ "," +u.getFriendsCount()+ "," +u.getStatusesCount()+","+System.currentTimeMillis()+"\n");
						//followerFile.write(u.getId()+ ",\"" +u.getScreenName()+"\","+ u.getName()+ "," +u.getFollowersCount()+","+System.currentTimeMillis()+"\n");
					}		
					followerFile.flush();
					Thread.sleep(1000*1);//sleep for 1 second
				}
				Thread.sleep(1000*(60-bins)*1);//sleep for 1 minute (less 1 second for each bin)
			}

			followerFile.close();
		}catch(Exception e){
			e.printStackTrace();
		}	
	}
	public void importFollowerFileIntoDB(String followerFile){
		Connection c = null;
		Statement stmt = null;
		try {
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());

			stmt = c.createStatement();
			//stmt.executeQuery("DROP INDEX IF EXISTS timeIndex ON BKG;");
			stmt.executeUpdate(" DROP TABLE IF EXISTS BKG ;");
			String sql = "CREATE TABLE  `BKG` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `SCREENNAME`           TEXT    NOT NULL, " + 
					" `NAME`            TEXT     NOT NULL, " + 
					" `FOLLOWERCOUNT`           INT    NOT NULL, " + 
					" `FRIENDCOUNT`           INT    NOT NULL, " + 
					" `STATUSCOUNT`           INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `timeIndex` ON `BKG` (`TIMESTAMP` ASC);"; 
			System.out.println(sql);
			stmt.executeUpdate(sql);
			InputStream    fis;
			BufferedReader br;
			fis = new FileInputStream(followerFile);//"D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/followerSnapshotsFile2.txt");
			br = new BufferedReader(new InputStreamReader(fis));
			String line;
			int i=0;
			while ((line=br.readLine())!=null){			
				String[] userInfo = line.split(",");	
				i++;
				sql = "INSERT INTO BKG (USERID,SCREENNAME,NAME,FOLLOWERCOUNT,FRIENDCOUNT,STATUSCOUNT,TIMESTAMP) " +
						"VALUES ("+userInfo[0]+",\""+userInfo[1].substring(1,userInfo[1].length()-1)+"\",\""+userInfo[2]+"\","+userInfo[3]+","+userInfo[4]+","+userInfo[5]+","+userInfo[6]+")"; 
				try{
					stmt.executeUpdate(sql);
				}catch(Exception ee){
					System.out.println(sql); 
					ee.printStackTrace();
				}
			}     
			stmt.close();
			//c.commit();
			c.close();
			br.close();

		} catch(Exception e) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			System.exit(0);
			e.printStackTrace();
		}

	}
	/*public void importStatusFileIntoDB(String LocationFile){
		Connection c = null;
		Statement stmt = null;
		try {
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection("jdbc:sqlite:test.db");

			stmt = c.createStatement();
			//stmt.executeQuery("DROP INDEX IF EXISTS timeIndex ON BKG;");
			stmt.executeUpdate(" DROP TABLE IF EXISTS BKGSts ;");
			String sql = "CREATE TABLE  `BKGSts` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `SCREENNAME`           TEXT    NOT NULL, " + 
					" `NAME`            TEXT     NOT NULL, " + 
					" `statuscount`           BIGINT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `StsTimeIndex` ON `BKGSts` (`TIMESTAMP` ASC);"; 
			System.out.println(sql);
			stmt.executeUpdate(sql);
			InputStream    fis;
			BufferedReader br;
			fis = new FileInputStream(LocationFile);//"D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/followerSnapshotsFile2.txt");
			br = new BufferedReader(new InputStreamReader(fis));
			String line;int i=0;
			while ((line=br.readLine())!=null){			
				String[] userInfo = line.split(",");	
				i++;
				sql = "INSERT INTO BKGSts (USERID,SCREENNAME,NAME,statuscount,TIMESTAMP) " +
						"VALUES ("+userInfo[0]+",\'"+userInfo[1].substring(1,userInfo[1].length()-1)+"\',\'"+userInfo[2]+"\',"+userInfo[3]+","+userInfo[4]+")"; 
				//System.out.println(sql);
				stmt.executeUpdate(sql);
			}     
			stmt.close();
			//c.commit();
			c.close();
			br.close();

		}catch(Exception e){
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			System.exit(0);
			e.printStackTrace();
		}

	}*/

	//run the full query of follower counts against remote source
	public static HashMap<Long,Integer> getFollowerListFromDB(long timeStamp){
		HashMap<Long,Integer> result=new HashMap<Long, Integer>();
		Connection c = null;
		Statement stmt = null;
		try {
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());
			c.setAutoCommit(false);
			stmt = c.createStatement();
			String sql="SELECT B.USERID, B.FOLLOWERCOUNT "+
					" FROM (SELECT USERID, MAX(TIMESTAMP) AS MAXTS  FROM BKG  WHERE TIMESTAMP < "+timeStamp + 
					" GROUP BY USERID) A JOIN BKG B ON A.USERID=B.USERID AND A.MAXTS=B.TIMESTAMP";
			//sql="SELECT B.USERID, B.FOLLOWERCOUNT "+
			//		" FROM (SELECT USERID, MAX(TIMESTAMP) AS MAXTS  FROM copyBK  WHERE TIMESTAMP < "+timeStamp + 
			//		" GROUP BY USERID) A JOIN copyBK B ON A.USERID=B.USERID AND A.MAXTS=B.TIMESTAMP";
			//System.out.println(sql);
			ResultSet rs = stmt.executeQuery( sql);

			while ( rs.next() ) {
				long userId = rs.getLong("USERID");
				int followerCount  = rs.getInt("FOLLOWERCOUNT");
				result.put(userId, followerCount);
			}
			rs.close();
			stmt.close();
			c.close();
		} catch ( Exception e ) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			System.exit(0);
		}
		return result;
	}

	//run the full query of status counts against remote source
	public static HashMap<Long,Integer> getStsCountListFromDB(long timeStamp){
		HashMap<Long,Integer> result=new HashMap<Long, Integer>();
		Connection c = null;
		Statement stmt = null;
		try {
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());
			c.setAutoCommit(false);
			stmt = c.createStatement();
			String sql="SELECT B.USERID, B.STATUSCOUNT "+
					" FROM (SELECT USERID, MAX(TIMESTAMP) AS MAXTS  FROM BKG  WHERE TIMESTAMP < "+timeStamp + 
					" GROUP BY USERID) A JOIN BKG B ON A.USERID=B.USERID AND A.MAXTS=B.TIMESTAMP";
			//System.out.println(sql);
			ResultSet rs = stmt.executeQuery( sql);

			while ( rs.next() ) {
				long userId = rs.getLong("USERID");
				int stsCount  = rs.getInt("STATUSCOUNT");
				result.put(userId, stsCount);
			}
			rs.close();
			stmt.close();
			c.close();
		} catch ( Exception e ) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			System.exit(0);
		}
		return result;
	}


	public static int getUserFollowerFromDB(long timeStamp, long userID){
		int followers=0;
		Connection c = null;
		Statement stmt = null;
		try {
			//System.out.println("start of user follower count:");
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());
			c.setAutoCommit(false);
			stmt = c.createStatement();
			String sql="SELECT B.USERID, B.FOLLOWERCOUNT "+
					" FROM (SELECT USERID, MAX(TIMESTAMP) AS MAXTS  FROM BKG  WHERE TIMESTAMP < "+timeStamp + 
					" AND USERID= "+userID+" GROUP BY USERID) A JOIN BKG B ON A.USERID=B.USERID AND A.MAXTS=B.TIMESTAMP";  

			//sql="SELECT B.USERID, B.followerCut "+
			//		" FROM (SELECT USERID, MAX(TIMESTAMP) AS MAXTS  FROM copyBK  WHERE TIMESTAMP < "+timeStamp + 
			//		" AND USERID= "+userID+" GROUP BY USERID) A JOIN copyBK B ON A.USERID=B.USERID AND A.MAXTS=B.TIMESTAMP"; 
			//System.out.println(sql);
			ResultSet rs = stmt.executeQuery(sql );	      
			while ( rs.next() ) {
				followers  = rs.getInt("FOLLOWERCOUNT");	         
			}
			rs.close();
			stmt.close();
			c.close();
		} catch ( Exception e ) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			System.exit(0);
		}
		return followers;
	}
	public static int getUserStatusCountFromDB(long timeStamp, long userID){
		int followers=0;
		Connection c = null;
		Statement stmt = null;
		try {
			//System.out.println("start of user follower count:");
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());
			c.setAutoCommit(false);
			stmt = c.createStatement();
			String sql="SELECT B.USERID, B.STATUSCOUNT "+
					" FROM (SELECT USERID, MAX(TIMESTAMP) AS MAXTS  FROM BKG  WHERE TIMESTAMP < "+timeStamp + 
					" AND USERID= "+userID+" GROUP BY USERID) A JOIN BKG B ON A.USERID=B.USERID AND A.MAXTS=B.TIMESTAMP";  

			//System.out.println(sql);
			ResultSet rs = stmt.executeQuery(sql );	      
			while ( rs.next() ) {
				followers  = rs.getInt("STATUSCOUNT");	         
			}
			rs.close();
			stmt.close();
			c.close();
		} catch ( Exception e ) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			System.exit(0);
		}
		return followers;
	}


	public static HashMap<Long,Integer> getInitialUserFollowersFromDB(){
		HashMap<Long,Integer> result=new HashMap<Long, Integer>();
		Connection c = null;
		Statement stmt = null;
		try {
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());
			c.setAutoCommit(false);
			stmt = c.createStatement();
			String sql="SELECT B.USERID, B.FOLLOWERCOUNT, A.MINTS"+
					" FROM (SELECT USERID, MIN(TIMESTAMP) AS MINTS  FROM BKG  " + 
					" GROUP BY USERID) A JOIN BKG B ON A.USERID=B.USERID AND A.MINTS=B.TIMESTAMP";
			//sql="SELECT B.USERID, B.followerCut, A.MINTS"+
			//		" FROM (SELECT USERID, MIN(TIMESTAMP) AS MINTS  FROM copyBK  " + 
			//		" GROUP BY USERID) A JOIN copyBK B ON A.USERID=B.USERID AND A.MINTS=B.TIMESTAMP";
			System.out.println(sql);
			ResultSet rs = stmt.executeQuery( sql);

			while ( rs.next() ) {
				long userId = rs.getLong("USERID");
				int followerCount  = rs.getInt("FOLLOWERCOUNT");
				long timeStamp = rs.getLong("MINTS");
				result.put(userId, followerCount);
			}
			rs.close();
			stmt.close();
			c.close();
		} catch ( Exception e ) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			System.exit(0);
		}
		return result;
	}
	public static HashMap<Long,Integer> getInitialUserStatusCountFromDB(){
		HashMap<Long,Integer> result=new HashMap<Long, Integer>();
		Connection c = null;
		Statement stmt = null;
		try {
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());
			c.setAutoCommit(false);
			stmt = c.createStatement();
			String sql="SELECT B.USERID, B.STATUSCOUNT, A.MINTS"+
					" FROM (SELECT USERID, MIN(TIMESTAMP) AS MINTS  FROM BKG  " + 
					" GROUP BY USERID) A JOIN BKG B ON A.USERID=B.USERID AND A.MINTS=B.TIMESTAMP";
			System.out.println(sql);
			ResultSet rs = stmt.executeQuery( sql);

			while ( rs.next() ) {
				long userId = rs.getLong("USERID");
				int followerCount  = rs.getInt("STATUSCOUNT");
				long timeStamp = rs.getLong("MINTS");
				result.put(userId, followerCount);
			}
			rs.close();
			stmt.close();
			c.close();
		} catch ( Exception e ) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			System.exit(0);
		}
		return result;
	}
	public static void main(String[] args){
		TwitterFollowerCollector tfc=new TwitterFollowerCollector();
		//tfc.captureSnapshots("D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/followers.init","D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/followerSnapshotsFile.txt");
		//note that followerSnapshotFile should have been sorted based on timestamp

		tfc.importFollowerFileIntoDB(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"followerSnapshotsFile.txt");
		//		tfc.importStatusFileIntoDB(path+"acquaProj/StatusSnapshotsFile.txt");
		//HashMap<Long,Integer> initialCache = tfc.getInitialUserFollowersFromDB();
		//long time=new Long("1416244704221");
		//long userid= new Long("118288671");
		//HashMap<Long,Integer> list = tfc.getFollowerListFromDB(time); //gets the first window
		//int latestFollowerCount = tfc.getUserFollowerFromDB(time, userid);
		//System.out.println(latestFollowerCount + ">>>"+ list.size());
	}
}
