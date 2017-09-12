package acqua.data.generator.topk;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import acqua.config.Config;

public class BKGGenerator {

	private static List<Long> timestamps = new ArrayList<Long>();
	private static TreeMap<Long,Integer> followerCountChanges = new TreeMap<Long,Integer> ();  //key = timestamp , value = number of changes for all users
	private static TreeMap<Long,Integer> targetFollowerCountChanges = new TreeMap<Long,Integer> ();  //key = timestamp , value = number of changes for all users
	
	private static float avgOfChanges = 0 ;
	private static float targetAvgOfChanges = 1;   //should be changed to generate different data sets.
	
	
	private static List<Long> getTimestamps(){
		
		List<Long> list = new ArrayList<Long> ();
		Connection c = null;
		Statement stmt = null;
		try {
			Class.forName("org.sqlite.JDBC").newInstance();
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb() );
			c.setAutoCommit(false);
			stmt = c.createStatement();
			String sql="SELECT distinct B.TIMESTAMP FROM BKG B ORDER BY TIMESTAMP ";  
			ResultSet rs = stmt.executeQuery(sql );	      
			while ( rs.next() ) {
				long userID = rs.getLong("TIMESTAMP");
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

	//compute the number of changes for all users for each timestamp
	public static void ComputeChangsPerTimestamp() {
	
	long currentTimestamp = -1;
	int currentFollowerCount = 0;
	TreeMap<Long,Integer> currentFollowerList = new TreeMap<Long,Integer> ();  //key = userId , value =  followercount
	TreeMap<Long,Integer> previousFollowerList = new TreeMap<Long,Integer> (); 
	int previousFollowerCount = -1;
	
	// get all timestamps
	timestamps = getTimestamps();
	
	followerCountChanges.clear();
	
	//for each timestamp compute the number of changes for all users
	for ( int i = 0 ; i < timestamps.size() ; i++){
		
		currentTimestamp = timestamps.get(i);
		
		currentFollowerList = getFollowerListOfTimestamp(currentTimestamp);
		 Iterator<Long> it = currentFollowerList.keySet().iterator();
		 while(it.hasNext()){
			 Long userID = it.next();
			 currentFollowerCount = currentFollowerList.get(userID);
			 if (previousFollowerList.containsKey(userID))
				 previousFollowerCount = previousFollowerList.get(userID);
			
			 
			 if (currentFollowerCount != previousFollowerCount &&  i != 0){//previousFollowerCount != -1){
				 
				 if (followerCountChanges.containsKey(currentTimestamp)){
					 followerCountChanges.replace(currentTimestamp, followerCountChanges.get(currentTimestamp) + 1);
					// System.out.println("timestamp  " + currentTimestamp + "   user id   " + userID + "  currentFollowerCount " + currentFollowerCount + "  previousFollowerCount  " + previousFollowerCount);
				 }
				 else{
					 followerCountChanges.put(currentTimestamp, 1);  //first change of follower count for specific timestamp
					// System.out.println("timestamp  " + currentTimestamp + "   user id   " + userID + "  currentFollowerCount " + currentFollowerCount + "  previousFollowerCount  " + previousFollowerCount);
				 }
		 }
		 }
		
		previousFollowerList.putAll(currentFollowerList); 
	}
	computeAvgOfChanges();
	
}

	// give number of followers of each user in a given timestamp
	private static TreeMap<Long,Integer> getFollowerListOfTimestamp(long timestamp){
	
	TreeMap<Long,Integer> followers = new TreeMap<Long, Integer>();;
	Connection c = null;
	Statement stmt = null;
	try {
		Class.forName("org.sqlite.JDBC");
		c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb() );
		c.setAutoCommit(false);
		stmt = c.createStatement();
		
		String sql="SELECT B.USERID, B.FOLLOWERCOUNT "+
				" FROM (SELECT USERID, MAX(TIMESTAMP) AS MAXTS  FROM BKG  WHERE TIMESTAMP <=  " + timestamp + 
				" GROUP BY USERID) A JOIN BKG B ON A.USERID=B.USERID AND A.MAXTS=B.TIMESTAMP";
		
		ResultSet rs = stmt.executeQuery( sql );	      
		while ( rs.next() ) {
			long userID = rs.getLong("USERID");
			int followerCount  = rs.getInt("FOLLOWERCOUNT");
			followers.put( userID , followerCount);
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

	private static void computeAvgOfChanges(){
	
		int sum = 0;
		Iterator<Integer> it = followerCountChanges.values().iterator();
		while(it.hasNext()){
			 int changes = it.next();
			 sum = sum + changes;
			// System.out.println("sum= " + sum + "  i= " + i);
		}
		avgOfChanges = (float )sum / followerCountChanges.size();
		System.out.println("avgOfChanges= " + avgOfChanges + "  followerCountChanges size = " + followerCountChanges.size());
	}

	// compute the target number of changes of followerCount of all users in each timestamp based on the targetAvgOfChanges
	private static void computeTargetFollowerCountChanges() {
		
		int sum = 0;
		Iterator<Long> it = followerCountChanges.keySet().iterator();
		while(it.hasNext()){
			long time = it.next();
			int changes = followerCountChanges.get(time);
			int targetChanges = Math.round( (float)( changes * targetAvgOfChanges) / avgOfChanges);
			sum = sum + targetChanges;
			targetFollowerCountChanges.put(time, targetChanges);
		//	System.out.println("time= " + time + "  changes= " + changes + "  targetChanges= " + targetChanges);
		}
		
		targetAvgOfChanges = (float )sum / targetFollowerCountChanges.size();
		System.out.println("targetAvgOfChanges= " + targetAvgOfChanges + "  targetFollowerCountChanges size = " + targetFollowerCountChanges.size());
	}
	
	private static void modifyFollowerCountChanges() {
		
		if ( targetAvgOfChanges > avgOfChanges )
			increaseFollowerCountChanges();
		else
			decreaseFollowerCountChanges();
		
	}
	
	// decrease the average of the follower count changes in each timestamp
	private static void decreaseFollowerCountChanges() {

		long currentTS = -1 ;
		TreeMap<Long,Integer> currentFollowerList = new TreeMap<Long,Integer> ();  //key = userId , value =  followercount
		
		Random rand = new Random();
		int previousFollowerCount = 0 , nextFollowerCount = 0;
		
		Connection c = null;
		Statement stmt = null;
		try {
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb() );
			//c.setAutoCommit(false);
			stmt = c.createStatement();
			
			
		Iterator <Long> it = followerCountChanges.keySet().iterator();
		while(it.hasNext()){
			
			currentTS = (long)it.next();
			List<Long> userIDs = getUserIds(currentTS);
			currentFollowerList = getFollowerListOfTimestamp(currentTS);
			//List<Long> userIDs = new ArrayList<Long>(currentFollowerList.keySet());

			int numberOfChanges = followerCountChanges.get(currentTS) - targetFollowerCountChanges.get(currentTS)   ;   // number of changes to be decreased in order to reach the target changes			

			while (numberOfChanges > 0  && userIDs.size() > 0){
				
				long randomID = userIDs.get(rand.nextInt(userIDs.size()));
				int currentFollowerCount = currentFollowerList.get(randomID);
				
				// find previouse and next number of follower for the random user
				String sqlprevious="SELECT B.FOLLOWERCOUNT "+
						" FROM (SELECT USERID, MAX(TIMESTAMP) AS MAXTS  FROM BKG  WHERE TIMESTAMP < " + currentTS + 
						" AND  USERID = "+ randomID +  " ) A JOIN BKG B ON A.USERID=B.USERID AND A.MAXTS=B.TIMESTAMP";
				
				String sqlnext="SELECT B.FOLLOWERCOUNT "+
						" FROM (SELECT USERID, MIN(TIMESTAMP) AS MINTS  FROM BKG  WHERE TIMESTAMP > " + currentTS + 
						" AND USERID = "+ randomID +  " ) A JOIN BKG B ON A.USERID=B.USERID AND A.MINTS=B.TIMESTAMP";
				
				ResultSet rs = stmt.executeQuery( sqlprevious );	      
				while ( rs.next() ) {
					previousFollowerCount  = rs.getInt("FOLLOWERCOUNT");
				}
				
				rs = stmt.executeQuery( sqlnext );	      
				while ( rs.next() ) {
					nextFollowerCount = rs.getInt("FOLLOWERCOUNT");
				}
				
				//System.out.println("time =" + currentTS + "   ID =" + randomID + "   curr =" + currentFollowerCount + "   previous =" +previousFollowerCount + "   next =" + nextFollowerCount);
				
 				if (currentFollowerCount != previousFollowerCount && nextFollowerCount!= previousFollowerCount ){
					
					//System.out.println("user id = " + randomID + "change from " + currentFollowerCount +" to the value " + previousFollowerCount);
					updateFollowerCount( c ,randomID , currentTS , currentFollowerCount, previousFollowerCount );
					numberOfChanges--;
				}
 				userIDs.remove((Long)randomID);
 				
 				rs.close();
			}
			//System.out.println("  final number of changes = " + numberOfChanges);
			
		}
		stmt.close();
		c.close();
	} catch ( Exception e ) {
		System.err.println( e.getClass().getName() + ": " + e.getMessage() );
		System.exit(0);
	}
	}

	// increase the average of the follower count changes in each timestamp
	private static void increaseFollowerCountChanges() {
		
		long currentTS = -1 ;
		int previousFollowerCount = 0 , nextFollowerCount = 0 , newFollowerCount = 0;
		TreeMap<Long,Integer> currentFollowerList = new TreeMap<Long,Integer> ();  //key = userId , value =  followercount
		Random rand = new Random();
		
		Connection c = null;
		Statement stmt = null;
		
		try {
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb() );
			stmt = c.createStatement();
			
			
			Iterator<Long> it = followerCountChanges.keySet().iterator();
			while(it.hasNext()){
			
				currentTS = (long)it.next();
				currentFollowerList = getFollowerListOfTimestamp(currentTS);
				//List<Long> userIDs = new ArrayList<Long>(currentFollowerList.keySet());
				List<Long> userIDs = getUserIds(currentTS);
				int numberOfChanges = targetFollowerCountChanges.get(currentTS) - followerCountChanges.get(currentTS);   // number of changes to be decreased in order to reach the target changes
				
				while (numberOfChanges > 0  && userIDs.size() > 0){
					
					long randomID = userIDs.get(rand.nextInt(userIDs.size()));
					int currentFollowerCount = currentFollowerList.get(randomID);
					
					// find previouse and next number of follower for the random user
					String sqlprevious="SELECT B.FOLLOWERCOUNT "+
							" FROM (SELECT USERID, MAX(TIMESTAMP) AS MAXTS  FROM BKG  WHERE TIMESTAMP < " + currentTS + 
							" AND  USERID = "+ randomID +  " ) A JOIN BKG B ON A.USERID=B.USERID AND A.MAXTS=B.TIMESTAMP";
					
					String sqlnext="SELECT B.FOLLOWERCOUNT "+
							" FROM (SELECT USERID, MIN(TIMESTAMP) AS MINTS  FROM BKG  WHERE TIMESTAMP > " + currentTS + 
							" AND USERID = "+ randomID +  " ) A JOIN BKG B ON A.USERID=B.USERID AND A.MINTS=B.TIMESTAMP";
					
					ResultSet rs = stmt.executeQuery( sqlprevious );	      
					while ( rs.next() ) {
						previousFollowerCount  = rs.getInt("FOLLOWERCOUNT");
					}
					
					rs = stmt.executeQuery( sqlnext );	      
					while ( rs.next() ) {
						nextFollowerCount = rs.getInt("FOLLOWERCOUNT");
					}
					
					//System.out.println("time =" + currentTS + "   ID =" + randomID + "   curr =" + currentFollowerCount + "   previous =" +previousFollowerCount + "   next =" + nextFollowerCount);
					
	 				if (currentFollowerCount == previousFollowerCount && currentFollowerCount == nextFollowerCount){
						
						if (nextFollowerCount-currentFollowerCount <= 1){
							newFollowerCount = currentFollowerCount - 1 ;
						}else{
							newFollowerCount = (int) (currentFollowerCount + nextFollowerCount)/2 ;
						}	
						//System.out.println("user id = " + randomID + "  change from " + currentFollowerCount +" to the value " + newFollowerCount + "  at time " + currentTS);
						updateFollowerCount( c ,randomID , currentTS , currentFollowerCount, newFollowerCount);
						numberOfChanges--;
					}
	 				userIDs.remove((Long)randomID);
	 				rs.close();
				}
				
			}
			stmt.close();
			c.close();
		} catch ( Exception e ) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			System.exit(0);
		}
	
	}

	//update follower count in DB
	private static void updateFollowerCount( Connection c , long userID, long currentTS, int currentFollowerCount, int newFollowerCount) {
		
		Statement stmt = null;
		Statement stmt2= null;
		try {
			Class.forName("org.sqlite.JDBC");
			stmt = c.createStatement();
			
			String sql="SELECT TIMESTAMP , FOLLOWERCOUNT FROM BKG "
					+ " WHERE USERID = "+ userID +  " AND TIMESTAMP >= " + currentTS + "  ORDER BY TIMESTAMP" ;
			
			ResultSet rs = stmt.executeQuery( sql);	      
			while ( rs.next() ) {
				int followerCount  = rs.getInt("FOLLOWERCOUNT");
				long timestamp  = rs.getLong("TIMESTAMP");
				
				if ( followerCount != currentFollowerCount){
					//System.out.println( "break");
					break;
				}
				else{
					String updatesql = 	" UPDATE BKG SET FOLLOWERCOUNT = "+ newFollowerCount +"  WHERE USERID = " + userID + " AND TIMESTAMP =  " + timestamp ;
					//System.out.println(updatesql + "\n");
					stmt2 = c.createStatement();
					stmt2.executeUpdate(updatesql);
					stmt2.close();
				}
				
			}
			rs.close();
			stmt.close();
		} catch ( Exception e ) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			System.exit(0);
		}		
	}

	// give user IDs in a given timestamp
	private static List<Long> getUserIds(long timestamp){
		
		List<Long> userIDs = new ArrayList<Long>() ;
		Connection c = null;
		Statement stmt = null;
		try {
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb() );
			c.setAutoCommit(false);
			stmt = c.createStatement();
		
			String sql="SELECT USERID  FROM BKG  WHERE TIMESTAMP = "+ timestamp ;
			
			ResultSet rs = stmt.executeQuery( sql );	      
			while ( rs.next() ) {
				long userID = rs.getLong("USERID");
				
				userIDs.add(userID);
			}
			rs.close();
			stmt.close();
			c.close();
		} catch ( Exception e ) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			System.exit(0);
		}
		return userIDs;

	}
	
	
	
	public static void main(String[] args){

		ComputeChangsPerTimestamp();
		computeTargetFollowerCountChanges ();
		modifyFollowerCountChanges();
		ComputeChangsPerTimestamp();
	}
	

}
