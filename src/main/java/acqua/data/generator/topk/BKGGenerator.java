package acqua.data.generator.topk;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

import acqua.config.Config;

public class BKGGenerator {

	private static TreeMap<Long,Integer> followerCountChanges = new TreeMap<Long,Integer> ();  //key = timestamp , value = number of changes for all users
	private static float avgOfChanges = 0 ;
	
	private static TreeMap<Long,Integer> targetFollowerCountChanges = new TreeMap<Long,Integer> ();  //key = timestamp , value = number of changes for all users
	private static float targetAvgOfChanges = 3;
	
	private static List<Long> timestamps = new ArrayList<Long>();
	
	private static TreeMap<Long,Integer> usersForModification = new TreeMap<Long,Integer> ();  //key = userId , value = follower count
	

	
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

	public static void setChangsPerTimestamp(){
	
	long currentTimestamp = -1;
	int currentFollowerCount = 0;
	TreeMap<Long,Integer> currentFollowerList = new TreeMap<Long,Integer> ();  //key = userId , value =  followercount
	TreeMap<Long,Integer> previousFollowerList = new TreeMap<Long,Integer> (); 
	int previousFollowerCount = -1;
	// get all timestamps
	timestamps = getTimestamps();
	
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
				 
				 if (followerCountChanges.containsKey(currentTimestamp))
					 followerCountChanges.replace(currentTimestamp, followerCountChanges.get(currentTimestamp) + 1);
				 else
					 followerCountChanges.put(currentTimestamp, 1);  //first change of follower count for specific timestamp
		 }
		 }
		
		previousFollowerList.putAll(currentFollowerList); 
	}
	setAvgOfChanges();
	
}

	private static TreeMap<Long,Integer> getFollowerListOfTimestamp(long timestamp){
	
	TreeMap<Long,Integer> followers = new TreeMap<Long, Integer>();;
	Connection c = null;
	Statement stmt = null;
	try {
		Class.forName("org.sqlite.JDBC");
		c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb() );
		c.setAutoCommit(false);
		stmt = c.createStatement();
		//String sql="SELECT B.USERID, B.FOLLOWERCOUNT "+
		//		" FROM BKG B WHERE B.TIMESTAMP = "+ timestamp ;  
		
		String sql="SELECT B.USERID, B.FOLLOWERCOUNT "+
				" FROM (SELECT USERID, MAX(TIMESTAMP) AS MAXTS  FROM BKG  WHERE TIMESTAMP <=  "+timestamp + 
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

	private static void setAvgOfChanges(){
	
		int sum = 0;
		Iterator<Integer> it = followerCountChanges.values().iterator();
		while(it.hasNext()){
			 int changes = it.next();
			 sum = sum + changes;
			// System.out.println("sum= " + sum + "  i= " + i);
		}
		avgOfChanges = (float )sum / followerCountChanges.size();
		 System.out.println("avgOfChanges= " + avgOfChanges + "  followerCountChanges.size()= " + followerCountChanges.size());
	}

	private static void printFollowerCountChanges(){
		
		Iterator<Long> it = followerCountChanges.keySet().iterator();
		while(it.hasNext()){
			long time = it.next();
			int changes = followerCountChanges.get(time);
			System.out.println("time= " + time + "  changes= " + changes);
		}
		
		System.out.println("Avg= " + avgOfChanges + " FollowerCountChanges.size() " + followerCountChanges.size());
	}

	private static void computeTargetFollowerCountChanges(){
		
		int sum = 0;
		Iterator<Long> it = followerCountChanges.keySet().iterator();
		while(it.hasNext()){
			long time = it.next();
			int changes = followerCountChanges.get(time);
			int targetChanges = Math.round( (float)( changes * targetAvgOfChanges) / avgOfChanges);
			sum = sum + targetChanges;
			targetFollowerCountChanges.put(time, targetChanges);
			System.out.println("time= " + time + "  changes= " + changes + "  targetChanges= " + targetChanges);
		}
		
		targetAvgOfChanges = (float )sum / targetFollowerCountChanges.size();
		 System.out.println("targetAvgOfChanges= " + targetAvgOfChanges + "  targetFollowerCountChanges.size()= " + targetFollowerCountChanges.size());
	}
	
	private static void modifyFollowerCountChanges(){
		
		if ( targetAvgOfChanges > avgOfChanges )
			increaseFollowerCountChanges();
		else
			decreaseFollowerCountChanges();
	}
	
	
	
	private static void decreaseFollowerCountChanges() {

		long currentTS = -1 , previousTS = -1, nextTS = -1;
		TreeMap<Long,Integer> currentFollowerList = new TreeMap<Long,Integer> ();  //key = userId , value =  followercount
		TreeMap<Long,Integer> previousFollowerList = new TreeMap<Long,Integer> ();  //key = userId , value =  followercount
		TreeMap<Long,Integer> nextFollowerList = new TreeMap<Long,Integer> ();  //key = userId , value =  followercount
		Random rand = new Random();
		int previousFollowerCount = 0 , nextFollowerCount = 0;
		
		HashMap <String,Integer> result= new HashMap<String, Integer>();;
		Connection c = null;
		Statement stmt = null;
		try {
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb() );
			c.setAutoCommit(false);
			stmt = c.createStatement();
			
			
		Iterator it = followerCountChanges.keySet().iterator();
		while(it.hasNext()){
			
			currentTS = (long)it.next();
			currentFollowerList = getFollowerListOfTimestamp(currentTS);
			List<Long> userIDs = new ArrayList<Long>(currentFollowerList.keySet());

			int numberOfChanges = followerCountChanges.get(currentTS) - targetFollowerCountChanges.get(currentTS)   ;   // number of changes to be decreased in order to reach the target changes
			//System.out.println ("initil number Of Changes = " + numberOfChanges);
			

			while (numberOfChanges > 0  && userIDs.size() > 0){
				
				long randomID = userIDs.get(rand.nextInt(userIDs.size()));
				int currentFollowerCount = currentFollowerList.get(randomID);
				
				// HashMap<String,Integer> result =  findPreviousAndNextFollowerCount (currentTS , randomID);
				
				String sqlprevious="SELECT B.FOLLOWERCOUNT "+
						" FROM (SELECT USERID, MAX(TIMESTAMP) AS MAXTS  FROM BKG  WHERE TIMESTAMP < " + currentTS + 
						" AND  USERID = "+ randomID +  " ) A JOIN BKG B ON A.USERID=B.USERID AND A.MAXTS=B.TIMESTAMP";
				
				String sqlnext="SELECT B.FOLLOWERCOUNT "+
						" FROM (SELECT USERID, MIN(TIMESTAMP) AS MINTS  FROM BKG  WHERE TIMESTAMP > " + currentTS + 
						" AND USERID = "+ randomID +  " ) A JOIN BKG B ON A.USERID=B.USERID AND A.MINTS=B.TIMESTAMP";
				
				ResultSet rs = stmt.executeQuery( sqlprevious );	      
				while ( rs.next() ) {
					previousFollowerCount  = rs.getInt("FOLLOWERCOUNT");
					//System.out.println("previouse followerCount = " + previousFollowerCount);
				}
				
				rs = stmt.executeQuery( sqlnext );	      
				while ( rs.next() ) {
					nextFollowerCount = rs.getInt("FOLLOWERCOUNT");
					//System.out.println("next followerCount = " + nextFollowerCount);
				}
				
				//System.out.println("time =" + currentTS + "   ID =" + randomID + "   curr =" + currentFollowerCount + "   previous =" +previousFollowerCount + "   next =" + nextFollowerCount);
				
 				if (currentFollowerCount != previousFollowerCount && nextFollowerCount!= previousFollowerCount ){
					
					usersForModification.put(randomID , previousFollowerCount); 
					//System.out.println("user id = " + randomID + "change from " + currentFollowerCount +" to the value " + previousFollowerCount);
					numberOfChanges--;
				}
 				userIDs.remove((Long)randomID);
 				
 				rs.close();
			}
			//System.out.println("  final number of changes = " + numberOfChanges);
			
		}
		
		//rs.close();
		stmt.close();
		c.close();
	} catch ( Exception e ) {
		System.err.println( e.getClass().getName() + ": " + e.getMessage() );
		System.exit(0);
	}
	}

	private static HashMap<String,Integer> findPreviousAndNextFollowerCount (long timestamp , long userID){
		
		HashMap <String,Integer> result= new HashMap<String, Integer>();;
		Connection c = null;
		Statement stmt = null;
		try {
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb() );
			c.setAutoCommit(false);
			stmt = c.createStatement();
			
			String sqlprevious="SELECT B.FOLLOWERCOUNT "+
					" FROM (SELECT USERID, MAX(TIMESTAMP) AS MAXTS  FROM BKG  WHERE TIMESTAMP < " + timestamp + 
					" AND  USERID = "+ userID +  " ) A JOIN BKG B ON A.USERID=B.USERID AND A.MAXTS=B.TIMESTAMP";
			
			String sqlnext="SELECT B.FOLLOWERCOUNT "+
					" FROM (SELECT USERID, MIN(TIMESTAMP) AS MINTS  FROM BKG  WHERE TIMESTAMP > " + timestamp + 
					" AND USERID = "+ userID +  " ) A JOIN BKG B ON A.USERID=B.USERID AND A.MINTS=B.TIMESTAMP";
			
			ResultSet rs = stmt.executeQuery( sqlprevious );	      
			while ( rs.next() ) {
				int followerCount  = rs.getInt("FOLLOWERCOUNT");
				result.put( "previous" , followerCount);
				System.out.println("previouse followerCount = " + followerCount);
			}
			
			rs = stmt.executeQuery( sqlnext );	      
			while ( rs.next() ) {
				int followerCount  = rs.getInt("FOLLOWERCOUNT");
				result.put( "next" , followerCount);
				System.out.println("next followerCount = " + followerCount);
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
	
	
	
//	private static void decreaseFollowerCountChanges() {
//
//		long currentTS = -1 , previousTS = -1, nextTS = -1;
//		TreeMap<Long,Integer> currentFollowerList = new TreeMap<Long,Integer> ();  //key = userId , value =  followercount
//		TreeMap<Long,Integer> previousFollowerList = new TreeMap<Long,Integer> ();  //key = userId , value =  followercount
//		TreeMap<Long,Integer> nextFollowerList = new TreeMap<Long,Integer> ();  //key = userId , value =  followercount
//		Random rand = new Random();
//		for ( int i = 1 ; i < timestamps.size()-1 ; i++){
//			
//			previousTS = timestamps.get(i-1);
//			currentTS = timestamps.get(i);
//			nextTS = timestamps.get(i+1);
//			
//			
//			previousFollowerList = getFollowerListOfTimestamp(previousTS);
//			currentFollowerList = getFollowerListOfTimestamp(currentTS);
//			nextFollowerList = getFollowerListOfTimestamp(nextTS);
//			
////			System.out.println("previousTS"+previousTS);
////			System.out.println("currentTS"+currentTS);
////			System.out.println("nextTS"+nextTS);
////			
//			if ( ! targetFollowerCountChanges.containsKey(currentTS) ||  !followerCountChanges.containsKey(currentTS))
//				continue;
//			
//			int numberOfChanges = followerCountChanges.get(currentTS) - targetFollowerCountChanges.get(currentTS)   ;   // number of changes to be decreased in order to reach the target changes
//			System.out.println ("numberOfChanges" + numberOfChanges);
//			
//			List<Long> userIDs = new ArrayList<Long>(currentFollowerList.keySet());
//
//			while (numberOfChanges > 0 ){
//				
//				long randomID = userIDs.get(rand.nextInt(userIDs.size()));
//				
//				// if random exist in all list
//				int currentFollowerCount = currentFollowerList.get(randomID);
//				int previousFollowerCount = previousFollowerList.get(randomID);
//				int nextFollowerCount = nextFollowerList.get(randomID);
//				
//				if (currentFollowerCount != previousFollowerCount && nextFollowerCount!= previousFollowerCount ){
//					
//					usersForModification.put(randomID , previousFollowerCount); 
//					System.out.println("user id = " + randomID + "change from " + currentFollowerCount +" to the value " + previousFollowerCount);
//					numberOfChanges--;
//				}
//			}
//		}
//			
//	}

	private static void changeUserFollowrCount(long randomID, int previousFollowerCount) {

		System.out.println("user id = " + randomID + "");		
	}

	private static void increaseFollowerCountChanges() {
		// TODO Auto-generated method stub
		
	}

	public static void main(String[] args){
		
		setChangsPerTimestamp();
		printFollowerCountChanges();	
		computeTargetFollowerCountChanges ();
		modifyFollowerCountChanges();
	}


}
