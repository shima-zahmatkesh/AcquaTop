package acqua.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import acqua.config.Config;
import twitter4j.ResponseList;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.User;
import twitter4j.conf.ConfigurationBuilder;








import java.nio.channels.FileChannel;
import java.nio.file.Files;



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
	
	public static long prevIntervalTimeStamp(long time, long userid){
		long result=0L;
		Connection c = null;
		Statement stmt = null;
		try {
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());
			c.setAutoCommit(false);
			stmt = c.createStatement();
			//String sql="select max(X.timestamp) as TS from (select bkg.tiMESTAMP as timestamp from bkg where bkg.tiMESTAMP<"+time+" and bkg.USERID="+userid+") as X";
			String sql="select min(bkg.tiMESTAMP) as MTS from bkg, (select max(X.timestamp) as TS,X.fc as FC from (select bkg.tiMESTAMP as timestamp ,bkg.foLLOWERCOUNT as fc from bkg where bkg.tiMESTAMP<"+time+" and bkg.USERID="+userid+") as X)as y where bkg.foLLOWERCOUNT =y.FC";
			ResultSet rs = stmt.executeQuery( sql);

			result = rs.getLong("MTS");
			if (result==0) result=time;
			stmt.close();
			c.close();
		} catch ( Exception e ) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			System.exit(0);
		}
		return result;
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
	
	public static Integer getUserFollowerFromDB(long timeStamp, long userID){
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

	public static long getPreviousExpTime(Long userId, Long timeStamp){
	long tpe=0L;
	Connection c = null;
	Statement stmt = null;
	try {
		//System.out.println("start of user follower count:");
		Class.forName("org.sqlite.JDBC");
		c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());
		c.setAutoCommit(false);
		stmt = c.createStatement();
		String sql="SELECT MAX(CHANGE) as tp FROM CHANGES WHERE USERID = "+userId+" AND CHANGE <= "+timeStamp;  

		ResultSet rs = stmt.executeQuery(sql );	      
		while ( rs.next() ) {
			tpe  = rs.getLong("tp");	         
		}
		rs.close();
		stmt.close();
		c.close();
	} catch ( Exception e ) {
		System.err.println( e.getClass().getName() + ": " + e.getMessage() );
		System.exit(0);
	}
	return tpe;
	}
	
	public static Long getUserNextExpFromDB(long timeStamp, long userID){
		
		Long nextExpT=Long.MAX_VALUE;
		Connection c = null;
		Statement stmt = null;
		try {
			//System.out.println("start of user follower count:");
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());
			c.setAutoCommit(false);
			stmt = c.createStatement();
			String sql="select min(bkg.TIMESTAMP) as mTS from bkg where bkg.USERID="+userID;
			ResultSet rs = stmt.executeQuery(sql);
			/*Long min = rs.getLong("mTS");
			sql="select max(bkg.TIMESTAMP) as MTS from bkg where bkg.USERID="+userID;
			rs = stmt.executeQuery(sql);
			Long max = rs.getLong("MTS");
			if (timeStamp>max ) return max;
			if (timeStamp<min) return min;*/
			sql="select min(bkg.timESTAMP) as TS from bkg where bkg.USERID= "+userID+" and bkg.tIMESTAMP> "+timeStamp+" and bkg.fOLLOWERCOUNT <> ("
					+" select bkg.foLLOWERCOUNT from bkg where bkg.uSERID= "+userID+" and bkg.tiMESTAMP<= "+timeStamp+" order by bkg.tIMESTAMP desc limit 1)";  

			//sql="SELECT B.USERID, B.followerCut "+
			//		" FROM (SELECT USERID, MAX(TIMESTAMP) AS MAXTS  FROM copyBK  WHERE TIMESTAMP < "+timeStamp + 
			//		" AND USERID= "+userID+" GROUP BY USERID) A JOIN copyBK B ON A.USERID=B.USERID AND A.MAXTS=B.TIMESTAMP"; 
//			System.out.println(sql);
			rs = stmt.executeQuery(sql );	      
			nextExpT  = rs.getLong("TS");	         
			rs.close();
			stmt.close();
			c.close();
		} catch ( Exception e ) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			System.exit(0);
		}
		if(nextExpT==0L) nextExpT=Long.MAX_VALUE;
		return nextExpT;
		
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

	public static HashMap<Long,Integer> getchangecount( ){
		HashMap<Long,Integer> changeCount=new HashMap<Long, Integer>();
		Connection c = null;
		Statement stmt = null;
		try {
			//System.out.println("start of user follower count:");
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());
			c.setAutoCommit(false);
			stmt = c.createStatement();
			String sql="SELECT A.USERID as userid, COUNT(*) as changecount FROM BKG A, BKG B WHERE A.TIMESTAMP-B.TIMESTAMP>0 AND A.TIMESTAMP-B.TIMESTAMP< 100000 AND A.USERID = B.USERID AND A.FOLLOWERCOUNT<>B.FOLLOWERCOUNT GROUP BY A.USERID";  

			//System.out.println(sql);
			ResultSet rs = stmt.executeQuery(sql );	      
			while ( rs.next() ) {
				changeCount.put(rs.getLong("userid"),rs.getInt("changecount"));
			}
			rs.close();
			stmt.close();
			c.close();
		} catch ( Exception e ) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			System.exit(0);
		}
		return changeCount;
	}
	
	public static HashMap<Long,String> getInitialUserFollowersFromDB(){
		HashMap<Long,String> result=new HashMap<Long, String>();
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
			//System.out.println(sql);
			ResultSet rs = stmt.executeQuery( sql);
			//System.out.println("followerCount start");
			while ( rs.next() ) {
				long userId = rs.getLong("USERID");
				int followerCount  = rs.getInt("FOLLOWERCOUNT");
				//System.out.println(followerCount);
				long timeStamp = rs.getLong("MINTS");
				result.put(userId, followerCount+","+timeStamp);
			}
			//System.out.println("followerCount end ");
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
	
	public static Long getMinimumFollowerCount(){
		Connection c = null;
		Statement stmt = null;
		long followerCount =0;
		try {
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());
			c.setAutoCommit(false);
			stmt = c.createStatement();
			String sql="SELECT  MIN(FOLLOWERCOUNT) AS MINFC  FROM BKG ";
			ResultSet rs = stmt.executeQuery( sql);
			followerCount  = rs.getLong("MINFC");
			rs.close();
			stmt.close();
			c.close();
		} catch ( Exception e ) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			System.exit(0);
		}
		return followerCount;
	}
	
	public static Long getMaximumFollowerCount(){
		Connection c = null;
		Statement stmt = null;
		long followerCount =0;
		try {
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());
			c.setAutoCommit(false);
			stmt = c.createStatement();
			String sql="SELECT  MAX(FOLLOWERCOUNT) AS MAXFC  FROM BKG ";
			ResultSet rs = stmt.executeQuery( sql);
			followerCount  = rs.getLong("MAXFC");
			rs.close();
			stmt.close();
			c.close();
		} catch ( Exception e ) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			System.exit(0);
		}
		return followerCount;
	}
	
	public static void main(String[] args){
		
		//TwitterFollowerCollector tfc=new TwitterFollowerCollector();
		//tfc.captureSnapshots("D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/followers.init","D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/followerSnapshotsFile2.txt");
		//note that followerSnapshotFile should have been sorted based on timestamp
		//tfc.importFollowerFileIntoDB(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"followerSnapshotsFile.txt");
		//tfc.importStatusFileIntoDB(path+"acquaProj/StatusSnapshotsFile.txt");
		//HashMap<Long,Integer> initialCache = tfc.getInitialUserFollowersFromDB();
		//long time=new Long("1416244704221");
		//long userid= new Long("118288671");
		//HashMap<Long,Integer> list = tfc.getFollowerListFromDB(time); //gets the first window
		//int latestFollowerCount = tfc.getUserFollowerFromDB(time, userid);
		//System.out.println(latestFollowerCount + ">>>"+ list.size());
		generateNewDB(60 , "realtesteveningDEC");   //manually copy and paste your source db file and name them xxxx_i.db which xxxx is the name of source file and i is from 1 to 10 and then call the function
		//copyfile();
	
	}

	
	public static void generateNewDB(int percentage , String dbName) {
	
		long userID = -1;
		int[] seeds = {10,11,12,13,14,15,16,17,18,19,20};

		for ( int i = 1 ; i <= 10 ; i++){
			Random rand = new Random(seeds[i]);
			String desDB = Config.INSTANCE.getDatasetDb().split("\\.")[0]+"_"+ i +".db" ;	
			System.out.println (desDB);
			List<Long> usersID = getUsersID( desDB );
			Iterator <Long> userIDIterator = usersID.iterator();
			
			while (userIDIterator.hasNext()) {
				
				userID = (Long) userIDIterator.next();
				
				//for monotonically decreasing DB
				//generatingNonDecreasingFollowerCountList (desDB , userID );
		
				
			    long randomNum = rand.nextInt(100);
				//System.out.println("randomNum = " +  randomNum);
			    if (randomNum < percentage){
			   
					HashMap<Long,Integer> followerList = getFollowerListOfUser(desDB , userID);	
					int minFollowerNum = getMinFollowerOfUser (followerList);
					int maxFollowerNum = getMaxFollowerOfUser (followerList);
					long maxAddedValue = Config.INSTANCE.getQueryFilterThreshold() - minFollowerNum;
					long minAddedValue = Config.INSTANCE.getQueryFilterThreshold() - maxFollowerNum;
					long randomValue = randLong( minAddedValue+1, maxAddedValue+1);
				
					updateFollowerListOfUser(desDB , userID , randomValue);		
					
					
			    }
			}
		}
		for (int j = 1 ; j<=10 ; j++){
			System.out.println("rename file from    " + Config.INSTANCE.getProjectPath()+ dbName +"_"+ j +".db" + "     to      " + Config.INSTANCE.getProjectPath()+ dbName +"_"+ j +"_"+percentage+".db");
			if (! renameFile (Config.INSTANCE.getProjectPath()+ dbName +"_"+ j +".db" , Config.INSTANCE.getProjectPath()+ dbName +"_"+ j +"_"+percentage+".db")  ){
				System.out.println("Error in renaming files");
			}
		}
	}

	
	public static List<Long> getUsersID( String DB){
		
		List<Long> list = new ArrayList<Long> ();
		Connection c = null;
		Statement stmt = null;
		try {
			Class.forName("org.sqlite.JDBC").newInstance();
			c = DriverManager.getConnection( DB );
			c.setAutoCommit(false);
			stmt = c.createStatement();
			String sql="SELECT distinct B.USERID FROM BKG B ";  
			ResultSet rs = stmt.executeQuery(sql );	      
			while ( rs.next() ) {
				long userID = rs.getLong("USERID");
				list.add(userID);
			}
			rs.close();
			stmt.close();
			c.close();
		} catch ( Exception e ) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			System.exit(0);
		}
		return list;
		
	}

	public static HashMap<Long,Integer> getFollowerListOfUser(String DB , long userID){
		
		HashMap<Long,Integer> followers = new HashMap<Long, Integer>();;
		Connection c = null;
		Statement stmt = null;
		try {
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection( DB );
			c.setAutoCommit(false);
			stmt = c.createStatement();
			String sql="SELECT B.TIMESTAMP, B.FOLLOWERCOUNT "+
					" FROM BKG B WHERE B.USERID = "+ userID ;  
			ResultSet rs = stmt.executeQuery( sql );	      
			while ( rs.next() ) {
				long timestamp = rs.getLong("TIMESTAMP");
				int followerCount  = rs.getInt("FOLLOWERCOUNT");
				followers.put( timestamp , followerCount);
				//System.out.println("followerCount = " + followerCount);
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
	
	private static int getMinFollowerOfUser (HashMap<Long,Integer> followersList){
		
		int min = -1;
		for (Integer follower : followersList.values()) {
		   if (follower < min || min == -1)
			   min = follower;		   
		}	
		return min;
	}
	
	private static int getMaxFollowerOfUser (HashMap<Long,Integer> followersList){
		
		int max = -1;
		for (Integer follower : followersList.values()) {
		   if (follower > max )
			   max = follower;		   
		}	
		return max;
	}

	public static long randLong(long min, long max) {
	    
	    Random rand = new Random();
	    long randomNum = rand.nextInt( (int) (max - min + 1) ) + min;
	    return randomNum;
	}
	
	private static void updateFollowerListOfUser( String DB , long userID , long randomValue){

		Connection c = null;
		Statement stmt = null;
		try {
			//System.out.println("start of user follower count:");
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection( DB );
			//c.setAutoCommit(false);
			stmt = c.createStatement();
			String sql= " UPDATE BKG  "+
						" SET FOLLOWERCOUNT = FOLLOWERCOUNT + (" + randomValue + " )"+
						" WHERE USERID = "+ userID ;  
			//System.out.println("user ID = " + userID);
			//System.out.println("random value = " + randomValue);
			//System.out.println(sql);
			int rs = stmt.executeUpdate( sql );	      
			//System.out.println ("update  " + rs +" rows");
			//rs.close();
			stmt.close();
			//c.commit();
			c.close();
		} catch ( Exception e ) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			System.exit(0);
		}
	
	}

	private static void generatingNonDecreasingFollowerCountList (String DB , long userID ){
		

		Connection c = null;
		Statement stmt = null;
		HashMap<Long,Integer> followers = new HashMap<Long, Integer>();;

		try {
			//System.out.println("start of user follower count:");
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection( DB );
			//c.setAutoCommit(false);
			stmt = c.createStatement();
			System.out.println("---------------------------" + userID + "-------------------------------");
			String sql="SELECT B.TIMESTAMP, B.FOLLOWERCOUNT "+
						" FROM BKG B WHERE B.USERID = "+ userID + " ORDER BY B.TIMESTAMP ASC" ;  

			long timestamp = -1;
			Integer followerCount = -1;	
			ResultSet rs = stmt.executeQuery( sql );	      
			while ( rs.next() ) {
				
				timestamp = rs.getLong("TIMESTAMP");				
				if (rs.getInt("FOLLOWERCOUNT") > followerCount )
					followerCount  = rs.getInt("FOLLOWERCOUNT");
				
				followers.put( timestamp , followerCount);
				System.out.println("time stamp" + timestamp + "         followerCount = " + followerCount);
			}
				
			//long timestamp = -1;
			//Integer followerCount = -1;
			
			System.out.println("---------------------------UPDATE                       " + userID + "-------------------------------");

			Iterator<Long> it= followers.keySet().iterator();
			while(it.hasNext()){
				
				timestamp = it.next();
				followerCount = followers.get(timestamp);
				
				String sql1= " UPDATE BKG  "+
						" SET FOLLOWERCOUNT = " + followerCount +
						" WHERE USERID = "+ userID  + " AND "+
						" TIMESTAMP = " + timestamp ;  
				//System.out.println("time stamp" + timestamp + "  followerCount = " + followerCount);
				//System.out.println("user ID = " + userID);
				//System.out.println("followerCount" + followerCount);
				//System.out.println(sql1);
				int rs1 = stmt.executeUpdate( sql1 );	 
					
			}
			//rs.close();
			stmt.close();
			//c.commit();
			c.close();
		} catch ( Exception e ) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			System.exit(0);
		}
	}

	
	public static HashMap<Long,String> getInitialUserFollowersFromDBForCache(){
		HashMap<Long,String> result=new HashMap<Long, String>();
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
			
			ResultSet rs = stmt.executeQuery( sql);
			while ( rs.next() ) {
				long userId = rs.getLong("USERID");
				int followerCount  = rs.getInt("FOLLOWERCOUNT");
				long timeStamp = rs.getLong("MINTS");
				if (followerCount >=  ( Config.INSTANCE.getQueryFilterThreshold() - Config.INSTANCE.getDistanceFromThreshold() ) ) {
					result.put(userId, followerCount+","+timeStamp);
				}
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
	
	public static boolean renameFile(String oldFile , String newFile){
		
		File oldfile =new File(oldFile);
		File newfile =new File(newFile);
		return oldfile.renameTo(newfile);
	}
	
	
	public static HashMap<Long,String> getInstanceSelectivity(){
		
		HashMap<Long,String> result=new HashMap<Long, String>();
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
			
			ResultSet rs = stmt.executeQuery( sql);
			while ( rs.next() ) {
				long userId = rs.getLong("USERID");
				int followerCount  = rs.getInt("FOLLOWERCOUNT");
				long timeStamp = rs.getLong("MINTS");
				if (followerCount >=  ( Config.INSTANCE.getQueryFilterThreshold() - Config.INSTANCE.getDistanceFromThreshold() ) ) {
					result.put(userId, followerCount+","+timeStamp);
				}
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
}
