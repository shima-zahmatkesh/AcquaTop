package acqua.query.join.MTKN;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import acqua.config.Config;
import acqua.data.TwitterFollowerCollector;

public class RNDJoinOperator extends ApproximateJoinMTKNOperator {

	protected int updateBudget = Config.INSTANCE.getUpdateBudget();

	
	@Override
	protected HashMap<Long, String> updatePolicy(Iterator<Long> candidateUserSetIterator,Map<Long,Long> usersTimeStampOfTheCurrentSlidedWindow, long evaluationTime) {
		
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
			int currentValue = TwitterFollowerCollector.getUserFollowerFromDB(evaluationTime, temp);
			if(followerReplica.get(temp)==currentValue)
				result.put(temp,"=");
			
			else{
				result.put(temp,"<>"+ currentValue + "  " + followerReplica.get(temp) );
				minTopK.addFollowerReplica (temp , currentValue );
			}
			counter++;
		}
		return result;
	}

		
		
}
