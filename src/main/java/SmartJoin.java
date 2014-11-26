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
import java.util.Random;
import java.util.Set;


public class SmartJoin extends ApproximateJoinOperator{
	protected int updateBudget;
	protected HashMap<Long,Float> UserChangeRate; 
	public SmartJoin(int ub) {
		updateBudget=ub;
		UserChangeRate=new HashMap<Long, Float>();
		Connection c = null;
	    Statement stmt = null;
	    try {
	      Class.forName("org.sqlite.JDBC");
	      c = DriverManager.getConnection("jdbc:sqlite:test.db");
	      c.setAutoCommit(false);
	      stmt = c.createStatement();
	      String sql="SELECT USERID, CHANGERATE from User ";
	      //System.out.println(sql);
	      ResultSet rs = stmt.executeQuery( sql);
	      
	      while ( rs.next() ) {
	         long userId = rs.getLong("USERID");
	         float userChangeRate  = rs.getFloat("CHANGERATE");
	         UserChangeRate.put(userId, userChangeRate);
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
	protected HashSet<Long> updatePolicy(Iterator<Long> candidateUserSetIterator){
		//decide which rows to update and return the list
		//it must satisfy the updateBudget constraint!
		final class User{
			long userId;
			float ExpirationTime;
			public User(long id,float et){userId=id;ExpirationTime=et;}
		}
		List<User> userExpirationTime=new ArrayList<User>();
		while(candidateUserSetIterator.hasNext()){
			long userid=Long.parseLong(candidateUserSetIterator.next().toString());
			float f= Float.parseFloat(UserChangeRate.get(userid).toString());
			long latestUpdateTime = Long.parseLong(userInfoUpdateTime.get(userid).toString());
			long expectedExpirationTime=latestUpdateTime+(long)(60000/f);
			//System.out.println(expectedExpirationTime);
			userExpirationTime.add(new User(userid, expectedExpirationTime));				
		}
		Collections.sort(userExpirationTime, new Comparator<User>() {
	        public int compare(User o1, User o2) {
	            return (int)(o1.ExpirationTime - o2.ExpirationTime);
	        }
	    });
		HashSet<Long> result=new HashSet<Long>();
		Iterator<User> it = userExpirationTime.iterator();
		int counter=0;
		while(it.hasNext()&&counter<updateBudget){
			User temp=it.next();
			System.out.println("user Id: "+temp.userId+" "+"Expiration Time: "+temp.ExpirationTime);
			result.add(temp.userId);
			counter++;
		}
		System.out.println("--------------------------------------------------");
		return result;
	}
}
