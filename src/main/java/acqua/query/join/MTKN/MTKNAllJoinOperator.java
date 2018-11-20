package acqua.query.join.MTKN;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import acqua.config.Config;
import acqua.data.RemoteBKGManager;
import acqua.data.TwitterFollowerCollector;

public class MTKNAllJoinOperator extends ApproximateJoinMTKNOperator{

protected int updateBudget = Config.INSTANCE.getUpdateBudget();

	@Override
	protected HashMap<Long, String> updatePolicy(	Iterator<Long> candidateUserSetIterator,
													Map<Long, Long> usersTimeStampOfTheCurrentSlidedWindow,
													long evaluationTime) {
		
		HashMap<Long,String> result=new HashMap<Long,String>();

		ArrayList<String> MTKNList = minTopK.getMTKNList();
		//minTopK.printMTKN();

		Iterator<String> it= MTKNList.iterator();
		while(it.hasNext() ){
		
			String temp = it.next();
			String[] splitTemp = temp.split(",");
			long userId = Long.parseLong(splitTemp[0]);

			Integer replicaValue = followerReplica.get(userId);
			if (replicaValue == null) replicaValue =0;
			
//			int currentValue =0;
//			
//			if(Config.INSTANCE.getDatabaseContext().equals("twitter")){
//				currentValue = TwitterFollowerCollector.getUserFollowerFromDB(evaluationTime, userId);
//			}
//			if(Config.INSTANCE.getDatabaseContext().equals("stock")){
//				currentValue = RemoteBKGManager.INSTANCE.getCurrentStockRevenueFromDB(evaluationTime, userId);
//			}
			
			int currentValue = TwitterFollowerCollector.getUserFollowerFromDB(evaluationTime, userId);
			
			if( replicaValue == currentValue )
				result.put(userId,"=" + currentValue );
			else{
				result.put(userId,"<>"+ currentValue + "  " + replicaValue );
				minTopK.addFollowerReplica (userId , currentValue );
			}

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
