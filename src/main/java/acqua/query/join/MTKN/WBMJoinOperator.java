package acqua.query.join.MTKN;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import acqua.config.Config;
import acqua.data.TwitterFollowerCollector;


public class WBMJoinOperator extends ApproximateJoinMTKNOperator {

	protected int updateBudget= Config.INSTANCE.getUpdateBudget();
	protected HashMap<Long,Double> userChangeRates; 
	//	private HashMap<Long, Long> userExactExp;
	private double hitratioWithinUpdateBudget;
	private double windowHitratio;
	

	public WBMJoinOperator() {
		userChangeRates=new HashMap<Long, Double>();
		Connection c = null;
		Statement stmt = null;
		try {
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());
			c.setAutoCommit(false);
			stmt = c.createStatement();
			String sql="";
			//createUserTableFromBKG();
			sql="SELECT USERID, CHANGERATE from User ";
			//sql="SELECT userid, changeRate from FollowerLowChr ";
			//CREATE TABLE FollowerLowChr AS SELECT copyBK.uSERID as userid, CAST(CAST(count(distinct copyBK.FollowerCut) as float)/30 AS FLOAT) as changeRate from copyBK group by copyBK.uSERID
//			System.out.println(sql);
			ResultSet rs = stmt.executeQuery( sql);

			try{
				answersFileWriter = new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/"+this.getClass().getSimpleName()+"Output.txt"));
				selectedCondidatesFileWriter= new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"Debug/"+this.getClass().getSimpleName()+"selectedupdateEntries.txt"));
				statsFileWriter= new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"Debug/"+this.getClass().getSimpleName()+"estimationErrorPerWindow.txt"));
				statsFileWriter.write("p,s,p&s,totalNumberOfCandidatesinML,numberOfExpiredCandidatesinML,numberOfExpiredCandidatesAfterTheMaintenanceinML,numberOfExpiredElementsInTheView,numberOfExpiredElementsInTheViewAfterTheMaintenance \n");
			}catch(Exception e){
				e.printStackTrace();
			}
			
			while ( rs.next() ) {
				long userId = rs.getLong("USERID");
				double userChangeRate  = rs.getDouble("CHANGERATE");
				userChangeRates.put(userId, userChangeRate);
			}
			rs.close();
			stmt.close();
			c.close();
		} catch ( Exception e ) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			System.exit(0);
		}
		// TODO Auto-generated constructor stub
	}

	public void createUserTableFromBKG(int interval){
		Connection c = null;
		Statement stmt = null;
		try {
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());
			//c.setAutoCommit(false);
			stmt = c.createStatement();
			String sql="";
			stmt.executeUpdate("Drop table IF EXISTS User");
			//---String sql = "CREATE TABLE  `User` as select bkg.USERID as USERID, cast(count(distinct(bkg.FOLLOWERCOUNT)) as real)/cast(count(distinct(bkg.tiMESTAMP)) as real) AS CHANGERATE from bkg group by bkg.USERID";
			stmt.executeUpdate("create table User ( USERID BIGINT, CHANGERATE real);");
			sql="insert into User SELECT A.USERID , round(cast(COUNT(*) as real)/cast((SELECT COUNT(TIMESTAMP) FROM BKG C WHERE C.USERID = A.USERID) as real),4) FROM BKG A, BKG B WHERE A.TIMESTAMP-B.TIMESTAMP>0 AND A.TIMESTAMP-B.TIMESTAMP< " + interval + " AND A.USERID = B.USERID AND A.FOLLOWERCOUNT<>B.FOLLOWERCOUNT GROUP BY A.USERID";
			//String sql="";
			System.out.println("creat user table");
			stmt.execute(sql);
			stmt.close();
			c.close();
		}catch(Exception e){e.printStackTrace();}
	}
	
	

	protected HashMap<Long,String> updatePolicy(Iterator<Long> candidateUserSetIterator,Map<Long,Long> usersTimeStampOfTheCurrentSlidedWindow, final long evaluationTime){
		//decide which rows to update and return the list
		//it must satisfy the updateBudget constraint!

		//the estimated expiration times
		List<User> userEstimatedCurrentExpirationTime=new ArrayList<User>();
		//number of expired users |X|
		int estimatedExpired = 0;

		//the real expiration times (for test purposes
		List<User> userExactExpirationTime=new ArrayList<User>();
		//the real expired element set rX
		HashSet<Long> actuallyExpiredUsers=new HashSet<Long>();

		//the return structure
		HashMap<Long,String> result=new HashMap<Long,String>();

		/********************************
		 * Compute the expiration times *
		 ********************************/
		while(candidateUserSetIterator.hasNext()){
			long userid=candidateUserSetIterator.next();
			//System.out.println(userid);
			if(isStale(evaluationTime, userid))actuallyExpiredUsers.add(userid);

			//for test purposes
			double f=1;
			int changeCount=0;
			try{
				//System.out.println(userid);
				f= userChangeRates.get(userid);
				changeCount=(int)Math.floor((double)1/f);
			}catch(Exception ee){
				//ee.printStackTrace(); 
				System.out.println("skip user "+ userid + "  because it will not expire");
				continue;
			}

			//read ~tp
			long lastUpdateTime = 
					userInfoUpdateTime.get(userid);
//					estimatedLastChangeTime.get(userid);
//					bkgLastChangeTime.get(userid);
			//long lastExactExpirationTime=TwitterFollowerCollector.getUserNextExpFromDB(lastUpdateTime, userid);
			
			//lastUpdateTime = TwitterFollowerCollector.prevIntervalTimeStamp(lastUpdateTime,userid);
			//long evaluationTimeAligned = TwitterFollowerCollector.prevIntervalTimeStamp(evaluationTime,userid);
			long nextExpirationTime=0L;
			long lastExpirationTime=0L;
			if(f>0){
			//Double numberOfChangeRateIntervalPassedAfterLastUpdate = Math.ceil((evaluationTime-lastUpdateTime)*(f/60000));
				Double numberOfChangeRateIntervalPassedAfterLastUpdate = Math.ceil((double)(evaluationTime-lastUpdateTime)/(double)(changeCount*60000));
			//nextExpirationTime=lastUpdateTime+(long)((float)(numberOfChangeRateIntervalPassedAfterLastUpdate*(60000/f)));
				nextExpirationTime=lastUpdateTime+(long)((float)(numberOfChangeRateIntervalPassedAfterLastUpdate*60000*changeCount));
			//lastExpirationTime=lastUpdateTime+(long)(60000/f);
				lastExpirationTime=lastUpdateTime+(long)(60000*changeCount);
			
			}else{
				lastExpirationTime=Long.MAX_VALUE;
				nextExpirationTime=Long.MAX_VALUE;
			}
			//f's unit is based on minutes inverse so to convert it into milisecond invert we have to multiply with 60000
			
			
			if(lastExpirationTime < evaluationTime)
				estimatedExpired++;

			
			
			userEstimatedCurrentExpirationTime.add(new User(userid,lastExpirationTime, nextExpirationTime,f));

			
		}

		/***************************************
		 * Filter the candidates *
		 ***************************************/
		List<User> expired = new ArrayList<User>();  
		List<User> notExpired = new ArrayList<User>();  

		for(User u : 
			userEstimatedCurrentExpirationTime
			//				userExactExpirationTime
				)
			if(u.expirationTime<=evaluationTime)
				expired.add(u);
			else
				notExpired.add(u);

		/***************************************
		 * Assign the scores to the candidates *
		 ***************************************/

		//sliding case
		if( usersTimeStampOfTheCurrentSlidedWindow!=null){
			//System.out.println("sliding--------------------------------------------");
			for(User u : expired){
				u.windowsBeforeExit = (int)Math.ceil(
						( usersTimeStampOfTheCurrentSlidedWindow.get(u.userId)
						+ Config.INSTANCE.getQueryWindowWidth() 
						- evaluationTime) 
						/ (Config.INSTANCE.getQueryWindowSlide()*1000) ); 
				u.windowsToLive = (int)Math.ceil(
						(u.nextExpirationTime 
						+ (60000/userChangeRates.get(u.userId)) 
						- evaluationTime) 
						/ (Config.INSTANCE.getQueryWindowSlide()*1000) );
				u.score = Math.min(u.windowsBeforeExit, u.windowsToLive);
				//System.out.println(u.windowsBeforeExit + "  " + u.windowsToLive);
			}
		}
		//tumbling case
//		else
//			{
//			//System.out.println("tumbling------------------------------------------------------------");
//			for(User u : expired){
//				u.score = 0;
//			}	
//			for(User u : notExpired){
//				u.score = 0 ;
//			}
//			}



		/************************
		 * Order the candidates *
		 ************************/

		Collections.sort(expired, new CandidateComparator());

		/******************
		 * Pick the top u *
		 ******************/

		Iterator<User> expiredIt = expired.iterator();
		
		int counter=0;
		int countHit=0;
		while(expiredIt.hasNext() && counter<updateBudget){
			User temp=expiredIt.next();
			//System.out.println("user Id: "+temp.userId+" "+"Expiration Time: "+temp.expirationTime);
			long replicaValue = this.followerReplica.get(temp.userId);
			long bkgValue = TwitterFollowerCollector.getUserFollowerFromDB(evaluationTime, temp.userId);
			if(replicaValue!=bkgValue)
			{
				result.put(temp.userId, "<>");
				//System.out.printf("Expired: id "+temp.userId+" estimatedxep>> %d changerate>> "+userChangeRates.get(temp.userId)+" chachedValue>> "+ replicaValue+ " actualValue>> "+bkgValue+" \n",(evaluationTime - temp.nextExpirationTime)/60000);
			}
			else
			{
				result.put(temp.userId, "=");
				//System.out.printf("Expired: id "+temp.userId+" estimatedxep>> %d changerate>> "+userChangeRates.get(temp.userId)+" chachedValue>> "+ replicaValue+ " actualValue>> "+bkgValue+" \n",(evaluationTime - temp.nextExpirationTime)/60000);
			}
			if(actuallyExpiredUsers.contains(temp.userId)) countHit++;
			//if(temp.expirationTime==0) continue;
			//if(currentValue==bkgValue) continue;
			//result.add(temp.userId);
			counter++;
		}
		/***********************************
		 * Fill the candidates with other  *
		 ***********************************/

		if(result.size()<updateBudget){
			
			Collections.sort(notExpired, new CandidateComparator());
			Iterator<User> notExpiredIt = notExpired.iterator();
			
			while(notExpiredIt.hasNext()&&counter<updateBudget){
				User temp=notExpiredIt.next();
				long currentValue = this.followerReplica.get(temp.userId);
				long bkgValue = TwitterFollowerCollector.getUserFollowerFromDB(evaluationTime, temp.userId);
				if(currentValue!=bkgValue){
					result.put(temp.userId,"<>" + currentValue + "  " + bkgValue);
					//System.out.printf("NOT Expired: id "+temp.userId+" >>estimatedexp >> %d changerate>> "+userChangeRates.get(temp.userId)+" cachedvalue>> "+currentValue+" actualValue "+bkgValue+" \n",(evaluationTime - temp.nextExpirationTime)/60000);
				} else {
					result.put(temp.userId,"=");
				//System.out.printf("NOT Expired: id "+temp.userId+" >>estimatedexp >> %d changerate>> "+userChangeRates.get(temp.userId)+" cachedvalue>> "+currentValue+" actualValue "+bkgValue+" \n",(evaluationTime - temp.nextExpirationTime)/60000);
				}
				if(actuallyExpiredUsers.contains(temp.userId)) 
					countHit++;
				counter++;
			}
		}

		/*******************************
		 * Compute and store the stats *
		 *******************************/

		hitratioWithinUpdateBudget=(double)countHit/(double)counter;
		Iterator<User> it = userEstimatedCurrentExpirationTime.iterator();
		counter=countHit=0;
		while(it.hasNext()){
			User temp=it.next();
			if(temp.expirationTime<evaluationTime)
			{
				if(actuallyExpiredUsers.contains(temp.userId))
					countHit++;
			}
		}
		windowHitratio=(double)countHit;


		try {
			statsFileWriter.write(expired.size()+","+actuallyExpiredUsers.size()+","+windowHitratio);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		return result;
	}

	final class User{
		long userId;
		long expirationTime, nextExpirationTime;

		int windowsBeforeExit, //L
		windowsToLive, //V
		score; //min(V,L)

		double changeRate;
		public User(long id,long te,long nte,double changeRate){
			userId=id;
			expirationTime=te;
			nextExpirationTime=nte;
			changeRate=changeRate;

			score = 0;
		}
	}

	private class CandidateComparator implements Comparator<User>{
		//<0 o1<o2
		//=0 o1=o2
		//>0 o1>o2
		public int compare(User o1, User o2) {
			int res = o2.score-o1.score;
			//int res = o1.score-o2.score;
			if(res!=0)
				return res;

			res=(int)(o1.expirationTime - o2.expirationTime);
			if(res==0){
				if(o2.changeRate==o1.changeRate) return 0;
				return (o2.changeRate-o1.changeRate)<0?-1:1;

			}
			return res;//users with the latest expiration time will come first and will be chosen
		}


		
	}


}
