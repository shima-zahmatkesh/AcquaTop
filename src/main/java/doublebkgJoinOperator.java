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

import acqua.query.join.ApproximateJoinOperator;
import acqua.query.join.JoinOperator;


public class doublebkgJoinOperator extends ApproximateDoubleJoinOperator  {
	protected int updateBudget;
	protected HashMap<Long,Float> StatusChangeRate; 
	protected HashMap<Long,Float> FollowerChangeRate;
	public doublebkgJoinOperator(int ub){
		updateBudget=ub;
		StatusChangeRate=new HashMap<Long, Float>();
		Connection c = null;
	    Statement stmt = null;
	    try {
	      Class.forName("org.sqlite.JDBC");
	      c = DriverManager.getConnection("jdbc:sqlite:test.db");
	      c.setAutoCommit(false);
	      stmt = c.createStatement();
	      String sql="SELECT userid, ChangeRate from  ";
	      sql="SELECT userid, changeRate from StatusCR ";
	      //CREATE TABLE FollowerLowChr AS SELECT copyBK.uSERID as userid, CAST(CAST(count(distinct copyBK.FollowerCut) as float)/30 AS FLOAT) as changeRate from copyBK group by copyBK.uSERID
	      System.out.println(sql);
	      ResultSet rs = stmt.executeQuery( sql);
	      
	      while ( rs.next() ) {
	         long userId = rs.getLong("userid");
	         float userChangeRate  = rs.getFloat("changeRate");
	         StatusChangeRate.put(userId, userChangeRate);
	      }
	      rs.close();
	      sql="SELECT userid, changeRate from FollowerLowChr ";
	      //CREATE TABLE FollowerLowChr AS SELECT copyBK.uSERID as userid, CAST(CAST(count(distinct copyBK.FollowerCut) as float)/30 AS FLOAT) as changeRate from copyBK group by copyBK.uSERID
	      System.out.println(sql);
	      ResultSet rs2 = stmt.executeQuery( sql);
	      
	      while ( rs2.next() ) {
	         long userId = rs2.getLong("userid");
	         float userChangeRate  = rs2.getFloat("changeRate");
	         FollowerChangeRate.put(userId, userChangeRate);
	      }
	      rs2.close();
	      stmt.close();
	      c.close();
	    } catch ( Exception e ) {
	      System.err.println( e.getClass().getName() + ": " + e.getMessage() );
	      System.exit(0);
	    }
	}
		//it return the list of users with 1 which shows that user from join counterpart 1 should be updated or 2 which shows that user from join counterpart2 should be updated 
		protected HashMap<Long, Integer> updatePolicyFollowerStatusCount(
				Iterator<Long> candidateUserSetIterator) {
			final class User{
				long userId;
				float statusExpirationTime;
				float followerExpirationTime;
				public User(long id,float set,float fet){userId=id;statusExpirationTime=set;followerExpirationTime=fet;}
			}
			List<User> userExpirationTime=new ArrayList<User>();
			while(candidateUserSetIterator.hasNext()){
				long userid=Long.parseLong(candidateUserSetIterator.next().toString());
				float f= Float.parseFloat(StatusChangeRate.get(userid).toString());
				long latestUpdateTime = Long.parseLong(StatusCountUpdateTime.get(userid).toString());
				long statusExpectedExpirationTime=latestUpdateTime+(long)(60000/f);
				long followerExpectedExpirationTime=latestUpdateTime+(long)(60000/f);
				//System.out.println(expectedExpirationTime);
				userExpirationTime.add(new User(userid, statusExpectedExpirationTime,followerExpectedExpirationTime));				
			}
			Collections.sort(userExpirationTime, new Comparator<User>() {
		        public int compare(User o1, User o2) {
		            return (int)(o1.ExpirationTime - o2.ExpirationTime);
		        }
		    });
			return null;
		}		
		
}
