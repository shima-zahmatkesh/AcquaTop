package acqua.query.join.acqua;

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

import twitter4j.UserList;
import acqua.config.Config;
import acqua.data.TwitterFollowerCollector;

public class LRUJoinOperator extends ApproximateJoinOperator{
	protected int updateBudget;
	private long currentTimestamp;
	
	public LRUJoinOperator(int ub){
		updateBudget=ub;
	}
	@Override
	public void process(long timeStamp, Map<Long, Integer> mentionList, Map<Long,Long> usersTimeStampOfTheCurrentSlidedWindow) {
		currentTimestamp =  timeStamp;
		super.process(timeStamp, mentionList,usersTimeStampOfTheCurrentSlidedWindow);
	}
	
	private double getchangerate(long userid){
		double cr=0;
		Connection c = null;
		Statement stmt = null;
		try {
			//System.out.println("start of user follower count:");
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());
			c.setAutoCommit(false);
			stmt = c.createStatement();
			String sql="SELECT changerate FROM User WHERE USERID = "+userid;  

			ResultSet rs = stmt.executeQuery(sql );	      
			while ( rs.next() ) {
				cr  = rs.getDouble("changerate");	         
			}
			rs.close();
			stmt.close();
			c.close();
		} catch ( Exception e ) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			System.exit(0);
		}
		return cr;
	}
	
	protected HashMap<Long,String> updatePolicy(Iterator<Long> candidateUserSetIterator,Map<Long,Long> usersTimeStampOfTheCurrentSlidedWindow, long evaluationTime){
		//decide which rows to update and return the list
		//it must satisfy the updateBudget constraint!
		final class User{
			long userId;
			long updateTimeDiff;
			public User(long id,long t){userId=id;updateTimeDiff=t;}
		}
		List<User> userUpdateLatency=new ArrayList<User>();

		
		while(candidateUserSetIterator.hasNext()){
			
			long userId=Long.parseLong(candidateUserSetIterator.next().toString());
			long latestUpdateTime = Long.parseLong(userInfoUpdateTime.get(userId).toString());
			userUpdateLatency.add(new User(userId, currentTimestamp-latestUpdateTime));			
			
		}
		
		
		
		Collections.sort(userUpdateLatency, new Comparator<User>() {

			public int compare(User o1, User o2) {
				int res=(int)(o2.updateTimeDiff - o1.updateTimeDiff);
				//if(res==0)
				//	res=(int)(o2.updateTimeDiff - o1.updateTimeDiff);
				return res;
			}
		});
		
//		System.out.println(" LRU final sorted result = " );
//		Iterator<User> it1 = userUpdateLatency.iterator();
//		while(it1.hasNext()){
//			User t = it1.next();
//			System.out.println("user Id =" + t.userId + "  score = " + t.updateTimeDiff );
//		}

		
		
		HashMap<Long,String> result=new HashMap<Long,String>();
		Iterator<User> it = userUpdateLatency.iterator();
		int counter=0;
		while(it.hasNext()&&counter<updateBudget){
			User temp = it.next();
			double replicaValue=followerReplica.get(temp.userId);
			double bkgValue=TwitterFollowerCollector.getUserFollowerFromDB(evaluationTime, temp.userId);
			if(replicaValue==bkgValue)
				{
				result.put(temp.userId,"=");
				}
			else 
				{
				result.put(temp.userId, "<>" + replicaValue + "  " + bkgValue);
				}
			//System.out.printf("id "+temp.userId+">>oldness "+temp.updateTimeDiff/60000+"  cr= "+getchangerate(temp.userId)+" chachedValue>> "+ replicaValue+ " actualValue>> "+bkgValue+" \n",(evaluationTime - temp.updateTimeDiff)/60000);
			counter++;
		}
		//System.out.println("skipped users: ");
		while(it.hasNext()){
			User temp = it.next();
			double replicaValue=followerReplica.get(temp.userId);
			double bkgValue=TwitterFollowerCollector.getUserFollowerFromDB(evaluationTime, temp.userId);
			//System.out.printf("id "+temp.userId+">>oldness "+temp.updateTimeDiff/60000+"  cr= "+getchangerate(temp.userId)+" chachedValue>> "+ replicaValue+ " actualValue>> "+bkgValue+" \n",(evaluationTime - temp.updateTimeDiff)/60000);
		}
		//System.out.println("time"+(evaluationTime-Config.INSTANCE.getQueryStartingTime())/60000+"--------------------------------------------------------------------------------------------------------------------");
		return result;
	}
	
	
	
}