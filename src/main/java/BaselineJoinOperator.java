import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;



public class BaselineJoinOperator extends ApproximateJoinOperator{
	protected int updateBudget;
	BaselineJoinOperator(int ub){
		updateBudget=ub;
	}
		protected HashSet<Long> updatePolicy(Iterator<Long> candidateUserSetIterator){
		//decide which rows to update and return the list
		//it must satisfy the updateBudget constraint!
			final class User{
				long userId;
				long updateTimeDiff;
				public User(long id,long t){userId=id;updateTimeDiff=t;}
			}
			List<User> userUpdateLatency=new ArrayList<User>();
			
			while(candidateUserSetIterator.hasNext()){
				long userid=Long.parseLong(candidateUserSetIterator.next().toString());
				long latestUpdateTime = Long.parseLong(userInfoUpdateTime.get(userid).toString());
				userUpdateLatency.add(new User(userid, System.currentTimeMillis()-latestUpdateTime));				
			}
			Collections.sort(userUpdateLatency, new Comparator<User>() {

		        public int compare(User o1, User o2) {
		            return (int)(o2.updateTimeDiff - o1.updateTimeDiff);
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