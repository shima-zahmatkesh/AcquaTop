package acqua.query.join.acqua;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import acqua.config.Config;
import acqua.data.TwitterFollowerCollector;
import acqua.query.join.scoringCombine.User;

public class FilterJoinOperator extends ApproximateJoinOperator {

	protected int updateBudget;
	
	public FilterJoinOperator(int ub){
		updateBudget=ub;
	}
	
	@Override
	protected HashMap<Long, String> updatePolicy(Iterator<Long> CandidateIds, Map<Long, Long> candidateUserSetIterator, long evaluationTime) {
		
		
		final class User{
			long userId;
			long filterDiff;
			public User(long id,long f){userId=id;filterDiff=f;}
		}
		
		List<User> userFilterDiff=new ArrayList<User>();
		while(CandidateIds.hasNext()){
			long userid= CandidateIds.next();
			long followerCount = followerReplica.get(userid);
				userFilterDiff.add(new User(userid,  Math.abs( followerCount - Config.INSTANCE.getQueryFilterThreshold() ) ));				
			
		}
		//Ascending Order
		Collections.sort(userFilterDiff, new Comparator<User>() {

			public int compare(User o1, User o2) {
				
				int res=(int) (o2.filterDiff - o1.filterDiff);
				//System.out.println("res = " + res);
				return -res;    
			}
		});

//		System.out.println("Filter final sorted result = " );
//		Iterator<User> it1 = userFilterDiff.iterator();
//		while(it1.hasNext()){
//			User t = it1.next();
//			System.out.println("user Id =" + t.userId + "  score = " + t.filterDiff );
//		}

		//////////////////////////////////////////
		
		HashMap<Long,String> result=new HashMap<Long,String>();
		Iterator<User> it = userFilterDiff.iterator();
		int counter=0;
		while(it.hasNext()&&counter<updateBudget){
			User temp = it.next();
			//System.out.println ("user id" + temp.userId + "dif" + temp.filterDiff);
			double replicaValue=followerReplica.get(temp.userId);
			double bkgValue=TwitterFollowerCollector.getUserFollowerFromDB(evaluationTime, temp.userId);
			if(replicaValue==bkgValue)
				{
				result.put(temp.userId,"=");
				}
			else 
				{
				result.put(temp.userId, "<>");
				}
			//System.out.println("id "+temp.userId+">> oldness "+ temp.filterDiff +"    chachedValue >> "+ replicaValue + "    actualValue >> "+ bkgValue +"    differenece >>" + ( bkgValue-replicaValue) + " \n");
			counter++;
		}
		//System.out.println("skipped users: ");
		while(it.hasNext()){
			User temp = it.next();
			double replicaValue=followerReplica.get(temp.userId);
			double bkgValue=TwitterFollowerCollector.getUserFollowerFromDB(evaluationTime, temp.userId);
			//System.out.printf("id "+temp.userId+">> oldness "+temp.filterDiff+"    chachedValue >> "+ replicaValue + "    actualValue >> "+ bkgValue +"    differenece >>" + ( bkgValue-replicaValue) + " \n");
		}
		return result;

	
	}

}
