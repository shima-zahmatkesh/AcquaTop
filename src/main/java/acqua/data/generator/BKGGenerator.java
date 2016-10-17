package acqua.data.generator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import acqua.config.Config;
import acqua.data.TwitterFollowerCollector;


public class BKGGenerator {

	protected int maxID;
	protected Random rand;
	Map<String, Double> intervalAssignment;
	Map<String, Double> transitionAssignment;
	Map<String, Double> initialValueAssignment;
	protected int minInterval;
	protected int maxInterval;
	protected int minTransition;
	protected int maxTransition;
	protected int minInitialValue;
	protected int maxInitialValue;

	public BKGGenerator(int maxID) {
		this.maxID = maxID;
		this.rand = new Random(System.currentTimeMillis());
		this.intervalAssignment = new TreeMap<String, Double>();
		this.transitionAssignment = new TreeMap<String, Double>();
		this.initialValueAssignment = new TreeMap<String, Double>();
	}

	public BKGGenerator(int maxID, Map<String, Double> intervalAssignment) {
		this(maxID);
		this.intervalAssignment = intervalAssignment;
	}
	
	public BKGGenerator(int maxID, int minI, int maxI, int minT, int maxT, int minInit, int maxInit) {
		this(maxID);
		this.minInterval = minI;
		this.maxInterval = maxI;
		generateIntervalAssignment( minI, maxI);
		this.minTransition = minT;
		this.maxTransition = maxT;
		generateTransitionAssignment( minT, maxT);
		this.minInitialValue = minInit;
		this.maxInitialValue = maxInit;
		generateInitialValueAssignment( minInit, maxInit);
	}

	public void generateIntervalAssignment( int min, int max) {
		
		for (int i = 1; i <= this.maxID; i++) {
			this.intervalAssignment.put(String.valueOf(i), (double) rand.nextInt(Math.abs(max- min)) + Math.min(min, max));
		}
	}
	
	public void generateTransitionAssignment( int min, int max) {
		
		for (int i = 1; i <= this.maxID; i++) {
			this.transitionAssignment.put(String.valueOf(i), (double) rand.nextInt(Math.abs(max- min)) + Math.min(min, max));
		}
	}
	
	public void generateInitialValueAssignment( int min, int max) {
		
		for (int i = 1; i <= this.maxID; i++) {
			this.initialValueAssignment.put(String.valueOf(i), (double) rand.nextInt(Math.abs(max- min)) + Math.min(min, max));
		}
	}

	public String changeFrequencyWriter() {
		String outputBase = "./data/BKG/";
		long currentTimeStamp = System.currentTimeMillis();
		File stat = new File(outputBase+"BKG_stat_"+currentTimeStamp+".txt");
		File outputFile = new File(outputBase+"BKG_"+currentTimeStamp+".txt");
		
		BufferedWriter wr;
		try {
			wr = new BufferedWriter(new FileWriter(stat));
			wr.write("Generated: " + getClass()+"\nmaxID: "+ this.maxID
					+"\nmin Interval: " + this.minInterval+"\nmax Interval: "+ this.maxInterval
					+"\nmin Transition: " + this.minTransition+"\nmax Transition: "+ this.maxTransition
					+"\nmin Initial Value: " + this.minInitialValue+"\nmax Initial Value: "+ this.maxInitialValue+"\n");
			wr.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			wr = new BufferedWriter(new FileWriter(outputFile));
			for (int i = 1; i <= maxID; i++) {
				wr.write("<http://myexample.org/S" + i + ">,"
						+ (intervalAssignment.get(String.valueOf(i)).intValue())+ ","
						+ (transitionAssignment.get(String.valueOf(i)).intValue())+ ","
						+ (initialValueAssignment.get(String.valueOf(i)).intValue())+ " \n");
			}
			wr.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
//		for (int i = 1; i <= maxID; i++) {
//			System.out.println("<http://myexample.org/S" + i + ">,"
//								+ (intervalAssignment.get(String.valueOf(i)).intValue())+ ","
//								+ (transitionAssignment.get(String.valueOf(i)).intValue())+ ","
//								+ (initialValueAssignment.get(String.valueOf(i)).intValue())+ " \n");
//		}
		System.out.println("Generated: " + getClass()+"\nmaxID: "+ this.maxID
							+"\nmin Interval: " + this.minInterval+"\nmax Interval: "+ this.maxInterval
							+"\nmin Transition: " + this.minTransition+"\nmax Transition: "+ this.maxTransition
							+"\nmin Initial Value: " + this.minInitialValue+"\nmax Initial Value: "+ this.maxInitialValue+"\n");
	
		return (outputBase+"BKG_"+currentTimeStamp+".txt");
	
	
	}

	public static TreeMap<Long,Integer> getFollowerListOfUser(String DB , long userID){
		
		TreeMap<Long,Integer> followers = new TreeMap<Long, Integer>();;
		Connection c = null;
		Statement stmt = null;
		try {
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection( DB );
			c.setAutoCommit(false);
			stmt = c.createStatement();
			String sql="SELECT B.TIMESTAMP, B.FOLLOWERCOUNT "+
					" FROM BKG B WHERE B.USERID = "+ userID +
					" ORDER BY B.TIMESTAMP ASC";  
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
	
	public static void importBKGFileToDB (String dbType){
		
		String db = Config.INSTANCE.getDatasetDb();
		List<Long> userIDs = new ArrayList<Long> ();
		BufferedReader br;
		String line = null;
		String sql = null;
		
		
		//generate BKG text file that indicate the interval and transition for each user
		BKGGenerator myBKG = new BKGGenerator(400,1,20,1,10,1000,10000);
		String BKGFileName = myBKG.changeFrequencyWriter();
		HashMap<Long,Integer>  modifiedFollowerListOfUser = new HashMap<Long,Integer> ();
		
		// get the initial timestamp of each user
		HashMap<Long,String> initialUsers = TwitterFollowerCollector.getInitialUserFollowersFromDB();
		
		
		//for each user get the list of followerCount and timestamps and modify in based on the BKG text file (interval and transition for each user)
		Iterator<Long> IDsIt= initialUsers.keySet().iterator();
		try{
			br = new BufferedReader(new InputStreamReader(new FileInputStream(BKGFileName)));
		while(IDsIt.hasNext()){
			
			Long userId = IDsIt.next();
			String tempInfo = initialUsers.get(userId);
			
			String[] initialInfo = tempInfo.split(",");
			int initialfollowerCount = Integer.valueOf(initialInfo[0]);
			Long initialTimestamp = Long.valueOf(initialInfo[1]);
			Random rand = new Random(System.currentTimeMillis());
			
			
				line = br.readLine();
			if( line != null ){
				
				String[] userInfo = line.split(",");
				String userURL = userInfo[0];
				int interval = Integer.valueOf(userInfo[1]);
				int transition = Integer.valueOf(userInfo[2]);
				
				System.out.println ("user id = " + userId + " interval = " + interval + " transition = " + transition);
				//System.out.println ("initial timestam = " + initialTimestamp + "initial follower = "+ initialfollowerCount );
				
				TreeMap<Long,Integer>  followerListOfUser = getFollowerListOfUser(db ,userId);
				Iterator<Long> followerIt= followerListOfUser.keySet().iterator();
				int intervalCounter = interval;
				int newFollowerCount = initialfollowerCount ;
				
				while(followerIt.hasNext()){
					
					Long timestamp = followerIt.next();
					Integer followerCount = followerListOfUser.get( timestamp );
					if (intervalCounter == 0){
						
						switch (dbType) {
				            case "INC":
				            	newFollowerCount = newFollowerCount + transition;
				                break;
				            case "DEC":
				            	newFollowerCount = newFollowerCount - transition;
				                break;
				            case "MIX"
				            	int randNum = rand.nextInt(2);
				            	if (randNum == 0) {newFollowerCount = newFollowerCount + transition;}
				            	else if (randNum == 1) {newFollowerCount= newFollowerCount - transition;}
				                break;
						}
						//newFollowerCount = + transition;
						intervalCounter = interval;
					}
					modifiedFollowerListOfUser.put(timestamp ,newFollowerCount);
					
					System.out.println ("timestamp =" +timestamp + "  old fc =  " + followerCount + "   new fc =  " + newFollowerCount);
							
					intervalCounter--;
					
				}
				Connection c = null;
				Statement stmt = null;
				
					//import modified value to DB
					Class.forName("org.sqlite.JDBC");
					c = DriverManager.getConnection(db);
					stmt = c.createStatement();
					
					Iterator<Long> it= modifiedFollowerListOfUser.keySet().iterator();
					while(it.hasNext()){
						Long timestamp = it.next();
						Integer followerCount = modifiedFollowerListOfUser.get( timestamp );
					
						sql = "UPDATE  BKG SET FOLLOWERCOUNT =" + followerCount + " WHERE TIMESTAMP =" + timestamp+ " AND USERID =" + userId;
						//System.out.println(sql);
						stmt.executeUpdate(sql);
					}
					
					stmt.close();
					//c.commit();
					c.close();
				
				
			}
			
		}
	} catch (Exception e) {e.printStackTrace(); }
		
	
	}
	
	public static void main(String[] agrs) {
		
		//BKGGenerator myBKG = new BKGGenerator(400,100,200,1,10,1000,10000);
		//myBKG.changeFrequencyWriter();
		importBKGFileToDB ("MIX");  // get the following input parameter ("INC", "DEC", "MIX") to generate increasing, decreasing , or mix transition of FollowerCont 
	}
	
	
	
}
