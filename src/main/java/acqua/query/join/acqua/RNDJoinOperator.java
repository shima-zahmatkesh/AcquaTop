package acqua.query.join.acqua;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import acqua.config.Config;
import acqua.data.TwitterFollowerCollector;


public class RNDJoinOperator extends ApproximateJoinOperator {
	
	protected int updateBudget;

	public RNDJoinOperator(int ub) {
	updateBudget=ub;
	// TODO Auto-generated constructor stub
}
	
	protected HashMap<Long,String> updatePolicy(Iterator<Long> candidateUserSetIterator,Map<Long,Long> usersTimeStampOfTheCurrentSlidedWindow, long evaluationTime ){
		//decide which rows to update and return the list
		//it must satisfy the updateBudget constraint!
		//A is the list of all userids avaiable in stream
		HashMap<Long,String> result=new HashMap<Long,String>();
		if(!candidateUserSetIterator.hasNext()) return result;
		ArrayList<Long> A=new ArrayList<Long>();

		while(candidateUserSetIterator.hasNext()){
			
			Long userId = candidateUserSetIterator.next();
			A.add(userId);	
			
		}
		if (A.size() == 0) return result ;
		Random rand = new Random(System.currentTimeMillis());
		ArrayList<Integer> indexes = new ArrayList<Integer>();
		while(indexes.size()<updateBudget){
			indexes.add(rand.nextInt(A.size()));
		}
		int counter=0;
		while(counter<updateBudget){
			Long temp = A.get(indexes.get(counter));
			if(followerReplica.get(temp)==TwitterFollowerCollector.getUserFollowerFromDB(evaluationTime, temp))
				result.put(temp,"=");
			else
				result.put(temp,"<>");
			counter++;
		}
		//System.out.println("-----------------------------------------------------------------");
		return result;
	}

}
