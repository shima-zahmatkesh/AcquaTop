package acqua.data.generator;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import acqua.config.Config;
import acqua.data.TwitterFollowerCollector;


// generate BKG data set  ( users and their number of followers in different timestamps ) from the data of LDBC data generator

public class BKGGenerator2 {
	
	static HashMap<Long,Long> userNumberofFollowers = new HashMap<Long,Long> ();
	static Long initialTimestamp = 0l; 
	
	
	
	private static void generateInitBKGTEMP (String PersonKnowsPersonFilePath){
		
		BufferedReader br;
		//extract number of followers
		try {
	
			br = new BufferedReader(new InputStreamReader(new FileInputStream(PersonKnowsPersonFilePath)));
			String line = br.readLine();
			line = br.readLine();
			while(line!=null){

				String[] lineSplit = line.split(Pattern.quote("|"));
				long userId1 = Long.parseLong(lineSplit[0]);
				long userId2 = Long.parseLong(lineSplit[1]);
				long time = generateTimestamp(lineSplit[2]);
				
				if (time > initialTimestamp)
					initialTimestamp = time;
				
				if (userNumberofFollowers.containsKey(userId1)){
					userNumberofFollowers.put(userId1, userNumberofFollowers.get(userId1)+1);
				}else{
					userNumberofFollowers.put(userId1, 1l);
				}
				
				if (userNumberofFollowers.containsKey(userId2)){
					userNumberofFollowers.put(userId2, userNumberofFollowers.get(userId2)+1);
				}else{
					userNumberofFollowers.put(userId2, 1l);
				}
				line = br.readLine();	
			}
			br.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("extranct number of followers done");
		
		//import number of followers in DB
		importFollowersDataIntoDB();
	}
	
	private static long generateTimestamp(String t) {
		
		t = t.replace('T', ' ');
		int index = t.indexOf('+', 0);
		String temp = t.substring(0, index);
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");
		long time = 0l;
		try {
			time = dateFormat.parse(temp).getTime();
			//System.out.println("time =  " + time ); 
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		};
		return time;
	}

	static private void importPersonsDataIntoDB(String personFilePath){
		
		BufferedReader br;
		Connection c = null;
		Statement stmt = null;
		try {
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());

			stmt = c.createStatement();
			stmt.executeUpdate(" DROP TABLE IF EXISTS PERSON ;");
			String sql = "CREATE TABLE  `PERSON` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `FIRSTNAME`           TEXT    NOT NULL, " + 
					" `LASTNAME`            TEXT     NOT NULL, " +
					" `CREATIONTIMESTAMP`        BIGINT NOT NULL); "; 
			//System.out.println(sql);
			stmt.executeUpdate(sql);
			System.out.println("creat table done");
			
			//extract all users and put in  DB
			try {
				
				br = new BufferedReader(new InputStreamReader(new FileInputStream(personFilePath)));

				String line = br.readLine();
				line = br.readLine();
				while(line!=null){
					
					//System.out.println(  line);
					String[] lineSplit = line.split(Pattern.quote("|"));
					long userId = Long.parseLong(lineSplit[0]);
					String firstName = lineSplit[1];
					String lastName = lineSplit[2];
					Long creationTime = generateTimestamp(lineSplit[5]);
					
					sql = "INSERT INTO PERSON (USERID,FIRSTNAME,LASTNAME,CREATIONTIMESTAMP) " +
							"VALUES ("+ userId +",\""+ firstName +"\",\""+ lastName +"\","+ creationTime +")";
					stmt.executeUpdate(sql);
					
					line = br.readLine();
				}
				br.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("import persons in DB done");     
			stmt.close();
			c.close();

		} catch(Exception e) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			System.exit(0);
			e.printStackTrace();
		}

	}

	static private void importFollowersDataIntoDB(){
		Connection c = null;
		Statement stmt = null;
		try {
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());

			stmt = c.createStatement();
			stmt.executeUpdate(" DROP TABLE IF EXISTS BKGTEMP ;");
			String sql = "CREATE TABLE  `BKGTEMP` ( " +
					" `USERID`           BIGINT    NOT NULL, " +  
					" `FOLLOWERCOUNT`           INT    NOT NULL, " + 
					//" `FRIENDCOUNT`           INT    NOT NULL, " + 
					//" `STATUSCOUNT`           INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); "; 
			
			//System.out.println(sql);
			stmt.executeUpdate(sql);
			
			System.out.println("creat table done");
			Iterator<Long> it=userNumberofFollowers.keySet().iterator();
			while (it.hasNext()){
				
				Long userId = it.next();
				Long followersNum = userNumberofFollowers.get(userId);
				if (followersNum == null) 
					followersNum = 0l;
			
		
				sql = "INSERT INTO BKGTEMP (USERID,FOLLOWERCOUNT,TIMESTAMP) " +
						"VALUES ("+ userId + ","+ followersNum +","+ initialTimestamp +")"; 
				try{
					stmt.executeUpdate(sql);
					
					//System.out.println(sql);
				}catch(Exception ee){
					System.out.println(sql); 
					ee.printStackTrace();
				}
			}     
			stmt.close();
			c.close();

		} catch(Exception e) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			System.exit(0);
			e.printStackTrace();
		}
		System.out.println("import number of followers in DB done");

	}
	
	static private void updateFollowers(String StreamFollowerFilePath){
		
		BufferedReader br;
		Connection c = null;
		Statement stmt = null;
		try {
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());

			stmt = c.createStatement();
			
		
				br = new BufferedReader(new InputStreamReader(new FileInputStream(StreamFollowerFilePath)));

				String line = br.readLine();
				line = br.readLine();
				while(line!=null){
					
					System.out.println(  line);
					String[] lineSplit = line.split(Pattern.quote("|"));
					long time = Long.parseLong(lineSplit[0]);
					long followerId = Long.parseLong(lineSplit[1]);
					long followedId = Long.parseLong(lineSplit[2]);					
					
					String sql = "SELECT FOLLOWERCOUNT , MAX(TIMESTAMP) FROM BKGTEMP WHERE USERID = " + followedId ;
						
					ResultSet rs = stmt.executeQuery( sql);
					long followercount = rs.getLong("FOLLOWERCOUNT");
					rs.close();
					stmt.close();
					long newFollowercount = followercount + 1;
					stmt = c.createStatement();
					sql = "INSERT INTO BKGTEMP (USERID,FOLLOWERCOUNT,TIMESTAMP) " +
							"VALUES ("+ followedId + ","+ newFollowercount +","+ time +")"; 
					stmt.executeUpdate(sql);
										
					line = br.readLine();
		
				}
				br.close();
				stmt.close();
				c.close();
		} catch(Exception e) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			System.exit(0);
			e.printStackTrace();
		}
	}
		
	static private void generateSnapshotFollowers(){
		
		Long timestamp = Config.INSTANCE.getSnapshotStart() ;
		Connection c = null;
		Statement stmt = null;
		
		try {
		
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());

			stmt = c.createStatement();
			stmt.executeUpdate(" DROP TABLE IF EXISTS BKG ;");
			String sql = "CREATE TABLE  `BKG` ( " +
					" `USERID`           BIGINT    NOT NULL, " +  
					" `FOLLOWERCOUNT`           INT    NOT NULL, " + 
					//" `FRIENDCOUNT`           INT    NOT NULL, " + 
					//" `STATUSCOUNT`           INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); "; 
						
			stmt.executeUpdate(sql);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		System.out.println("creat table done");
		
		
		for ( int i = 0 ; i < Config.INSTANCE.getExperimentIterationNumber()+30 ; i++){
			System.out.println("i = " + i + " timestamp = " + timestamp);
			HashMap<Long,Integer> userFollwerCount = getFollowerListFromDB(timestamp);
			
			
			//add Users With Zero Follower
			List<Long> usersID = getUsersID( Config.INSTANCE.getDatasetDb() );
			Iterator <Long> userIDIterator = usersID.iterator();
			System.out.println("all user ids = " + usersID.size());

			while (userIDIterator.hasNext()) {
				
				long userID = userIDIterator.next();
				if (!userFollwerCount.containsKey(userID)){
					userFollwerCount.put(userID, 0);
				}
			}
				
			System.out.println(userFollwerCount.size());
			Iterator<Long> it= userFollwerCount.keySet().iterator();
			while(it.hasNext()){
				
				long userId = it.next();
				Integer followerCount = userFollwerCount.get(userId);
				try {
					Class.forName("org.sqlite.JDBC");
					c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());

					stmt = c.createStatement();
					String sql = "INSERT INTO BKG (USERID,FOLLOWERCOUNT,TIMESTAMP) " +
								"VALUES ("+ userId + ","+ followerCount +","+ timestamp +")"; 
					stmt.executeUpdate(sql);
					stmt.close();
					c.close();
				} catch(Exception e) {
					System.err.println( e.getClass().getName() + ": " + e.getMessage() );
					System.exit(0);
					e.printStackTrace();
				}
				
			}
			timestamp += Config.INSTANCE.getIntervalSnapshot();
				
		}
	}
	
	private static HashMap<Long,Integer> getFollowerListFromDB(long timeStamp){
		HashMap<Long,Integer> result=new HashMap<Long, Integer>();
		Connection c = null;
		Statement stmt = null;
		try {
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());
			c.setAutoCommit(false);
			stmt = c.createStatement();
			String sql="SELECT B.USERID, B.FOLLOWERCOUNT "+
					" FROM (SELECT USERID, MAX(TIMESTAMP) AS MAXTS  FROM BKGTEMP  WHERE TIMESTAMP < "+timeStamp + 
					" GROUP BY USERID) A JOIN BKGTEMP B ON A.USERID=B.USERID AND A.MAXTS=B.TIMESTAMP";
			
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
	
	public static List<Long> getUsersID( String DB){
		
		List<Long> list = new ArrayList<Long> ();
		Connection c = null;
		Statement stmt = null;
		try {
			Class.forName("org.sqlite.JDBC").newInstance();
			c = DriverManager.getConnection( DB );
			c.setAutoCommit(false);
			stmt = c.createStatement();
			String sql="SELECT distinct B.USERID FROM PERSON B ";  
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
	
	static private void ImportCommentDataInDB(String CommentFilePath){
			
			BufferedReader br;
			Connection c = null;
			Statement stmt = null;
//			try {
//				Class.forName("org.sqlite.JDBC");
//				c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());
//				stmt = c.createStatement();
//				stmt.executeUpdate(" DROP TABLE IF EXISTS COMMENT ;");
//				String sql1 = "CREATE TABLE  `COMMENT` ( " +
//						" `COMMENTID`           BIGINT    NOT NULL, " + 
//						" `USERID`           BIGINT    NOT NULL ,"+
//						" FOREIGN KEY (USERID) REFERENCES PERSON(USERID) ); "; 
//				//System.out.println(sql);
//				stmt.executeUpdate(sql1);
//				System.out.println("creat table done");
//				
//			} catch (Exception e1) {
//				// TODO Auto-generated catch block
//				e1.printStackTrace();
//			}
			
			try {
				Class.forName("org.sqlite.JDBC");
				c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());
				stmt = c.createStatement();
				br = new BufferedReader(new InputStreamReader(new FileInputStream(CommentFilePath)));
				String line = br.readLine();
				//line = br.readLine();
				while(line!=null){
						
					System.out.println(  line);
					String[] lineSplit = line.split(Pattern.quote("|"));
					long commentId = Long.parseLong(lineSplit[0]);
					long userId = Long.parseLong(lineSplit[1]);					
						
					stmt = c.createStatement();
					String sql = "INSERT INTO COMMENT (COMMENTID , USERID) " +
								"VALUES ("+ commentId + ","+ userId +")"; 
					stmt.executeUpdate(sql);						
					line = br.readLine();
				}
				br.close();
				stmt.close();
				c.close();
				
			} catch(Exception e) {
				System.err.println( e.getClass().getName() + ": " + e.getMessage() );
				System.exit(0);
				e.printStackTrace();
			}
			

	}
	
	static private void ImportPostDataInDB(String PostFilePath){
		
		BufferedReader br;
		Connection c = null;
		Statement stmt = null;
//		try {
//			Class.forName("org.sqlite.JDBC");
//			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());
//			stmt = c.createStatement();
//			stmt.executeUpdate(" DROP TABLE IF EXISTS POST ;");
//			String sql1 = "CREATE TABLE  `POST` ( " +
//					" `POSTID`           BIGINT    NOT NULL, " + 
//					" `USERID`           BIGINT    NOT NULL ,"+
//					" FOREIGN KEY (USERID) REFERENCES PERSON(USERID) ); "; 
//			//System.out.println(sql);
//			stmt.executeUpdate(sql1);
//			System.out.println("creat table done");
//			
//		} catch (Exception e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
		
		try {
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());
			stmt = c.createStatement();
			br = new BufferedReader(new InputStreamReader(new FileInputStream(PostFilePath)));
			String line = br.readLine();
			while(line!=null){
					
				System.out.println(  line);
				String[] lineSplit = line.split(Pattern.quote("|"));
				long postId = Long.parseLong(lineSplit[0]);
				long userId = Long.parseLong(lineSplit[1]);					
					
				stmt = c.createStatement();
				String sql = "INSERT INTO POST (POSTID , USERID) " +
							"VALUES ("+ postId + ","+ userId +")"; 
				stmt.executeUpdate(sql);						
				line = br.readLine();
			}
			br.close();
			stmt.close();
			c.close();
			
		} catch(Exception e) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			System.exit(0);
			e.printStackTrace();
		}
		

}

	public static void main(String[] agrs) {
		
		// generate Person table
		//importPersonsDataIntoDB("/Users/zahmatkesh/git/datacollector/NewData/person_0_0.csv");
		
		//generate bkgtemp table
		//generateInitBKGTEMP( "/Users/zahmatkesh/git/datacollector/NewData/person_knows_person_0_0.csv");
		//updateFollowers("/Users/zahmatkesh/git/datacollector/NewData/stream_followers.csv");
		
		//generate bkg table
		generateSnapshotFollowers();
		
		//generate comment and post tables
		//ImportCommentDataInDB("/Users/zahmatkesh/git/datacollector/NewData/comment_hasCreator_person_0_0.csv");
		//ImportPostDataInDB("/Users/zahmatkesh/git/datacollector/NewData/post_hasCreator_person_0_0.csv");
		
		//genetare mentions table
		//ImportCommentDataInDB("/Users/zahmatkesh/git/datacollector/NewData/comment-creator-updates.csv");
		//ImportPostDataInDB("/Users/zahmatkesh/git/datacollector/NewData/post-creator-updates.csv");
		

		
		
		
		
		
		
		
		
		
		
		
		
		
//		FollowersChangeTime("/Users/zahmatkesh/git/datacollector/NewData/stream_followers.csv");
//		java.sql.Timestamp ts1 = new java.sql.Timestamp(1350203761822L);
//		System.out.println("ts :    " + ts1);
//		java.sql.Timestamp ts2 = new java.sql.Timestamp(1359673198967L);
//		System.out.println("ts :    " + ts2);
//		java.sql.Timestamp ts3 = new java.sql.Timestamp( 1350204059659L);
//		System.out.println("ts :    " + ts3);
//		java.sql.Timestamp ts4 = new java.sql.Timestamp( 1359673012051L);
//		System.out.println("ts :    " + ts4);
		
//		java.sql.Timestamp ts4 = new java.sql.Timestamp( 1350222934576L);
//		System.out.println("ts :    " + ts4);
		
		
		//Timestamp( "2016-02-01 11:11:02:000");
		
	}
	
	
	
	private static void Timestamp(String t) {
	
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:sss");
		long time = 0l;
		try {
			time = dateFormat.parse(t).getTime();
			//System.out.println("time =  " + time ); 
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		};
		System.out.println(time);
		java.sql.Timestamp ts = new java.sql.Timestamp(time);
		System.out.println("ts :                          " + ts);
	}
	
	static private void FollowersChangeTime(String StreamFollowerFilePath){
		
		BufferedReader br;
		Connection c = null;
		Statement stmt = null;
		try {
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());

			stmt = c.createStatement();
			
		
				br = new BufferedReader(new InputStreamReader(new FileInputStream(StreamFollowerFilePath)));

				String line = br.readLine();
				line = br.readLine();
				while(line!=null){
					
					String[] lineSplit = line.split(Pattern.quote("|"));
					long time = Long.parseLong(lineSplit[0]);
					java.sql.Timestamp ts = new java.sql.Timestamp(time);
					System.out.println( ts);
										
					line = br.readLine();
		
				}
				br.close();
				stmt.close();
				c.close();
		} catch(Exception e) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			System.exit(0);
			e.printStackTrace();
		}
	}
		
}
