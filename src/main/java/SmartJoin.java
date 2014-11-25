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
		Connection c = null;
	    Statement stmt = null;
	    try {
	      Class.forName("org.sqlite.JDBC");
	      c = DriverManager.getConnection("jdbc:sqlite:test.db");
	      c.setAutoCommit(false);
	      stmt = c.createStatement();
	      String sql="SELECT B.USERID, B.CHANGERATE from User ";
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
			float changeProb;
			public User(long id,float cp){userId=id;changeProb=cp;}
		}
		List<User> userUpdateLatency=new ArrayList<User>();
		while(candidateUserSetIterator.hasNext()){
			long userid=Long.parseLong(candidateUserSetIterator.next().toString());
			float f= Float.parseFloat(UserChangeRate.get(userid).toString());
			long latestUpdateTime = Long.parseLong(userInfoUpdateTime.get(userid).toString());
			userUpdateLatency.add(new User(userid, f*(System.currentTimeMillis()-latestUpdateTime)/60000));				
		}
		Collections.sort(userUpdateLatency, new Comparator<User>() {
	        public int compare(User o1, User o2) {
	            return (int)(o2.changeProb - o1.changeProb);
	        }
	    });
		HashSet<Long> result=new HashSet<Long>();
		Iterator<User> it = userUpdateLatency.iterator();
		int counter=0;
		while(it.hasNext()&&counter<updateBudget){
			result.add(it.next().userId);
			counter++;
		}
		return result;
	}
}
