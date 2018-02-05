package acqua.query.join.MTKN;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


import acqua.config.Config;
import acqua.data.TwitterFollowerCollector;

public class MTKNFJoinOperator extends ApproximateJoinMTKNOperator{

protected int updateBudget = Config.INSTANCE.getUpdateBudget();

	@Override
	protected HashMap<Long, String> updatePolicy(	Iterator<Long> candidateUserSetIterator,
													Map<Long, Long> usersTimeStampOfTheCurrentSlidedWindow,
													long evaluationTime) {
		
		HashMap<Long,String> result=new HashMap<Long,String>();

		ArrayList<String> topKResult = minTopK.getKMiddleResult(Config.INSTANCE.getK()) ;
		//minTopK.printMTKN();
		//System.out.println("middle result = " + topKResult.toString() );
		int counter = 0;
		Iterator<String> it= topKResult.iterator();
		while(it.hasNext() && counter< updateBudget){
		
			String temp = it.next();
			String[] splitTemp = temp.split(",");
			long userId = Long.parseLong(splitTemp[0]);

			Integer replicaValue = followerReplica.get(userId);
			if (replicaValue == null) replicaValue =0;
			
			int currentValue = TwitterFollowerCollector.getUserFollowerFromDB(evaluationTime, userId);
			
			if( replicaValue == currentValue )
				result.put(userId,"=" );
			else{
				result.put(userId,"<>"+ currentValue + "  " + replicaValue );
				minTopK.addFollowerReplica (userId , currentValue );
			}
			counter ++;
		}
//		System.out.println("-----update elemets of evaluation time = " + evaluationTime);
//		printResult(result);
		return result;
	}

	private void printResult(HashMap<Long, String> result) {
		
		Iterator <Long> it = result.keySet().iterator();
		while(it.hasNext()){
			long id = it.next();
			String res = result.get(id);
			System.out.println("id = " + id  + "  values = " + res);
		}
		
	}

}
