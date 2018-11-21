package acqua.query.join.MTKN;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import acqua.config.Config;
import acqua.data.TwitterFollowerCollector;


public class LRUJoinOperator extends ApproximateJoinMTKNOperator{
	
	protected int updateBudget = Config.INSTANCE.getUpdateBudget();
	private long currentTimestamp;

	
	
	@Override
	public void process(long timeStamp, Map<Long, Integer> mentionList, Map<Long,Long> usersTimeStampOfTheCurrentSlidedWindow) {
		currentTimestamp =  timeStamp;
		super.process(timeStamp, mentionList,usersTimeStampOfTheCurrentSlidedWindow);
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
		
		
		HashMap<Long,String> result=new HashMap<Long,String>();
		Iterator<User> it = userUpdateLatency.iterator();
		int counter=0;
		while(it.hasNext()&&counter<updateBudget){
			User temp = it.next();
			double replicaValue=followerReplica.get(temp.userId);
//			int bkgValue =0;
//			
//			if(Config.INSTANCE.getDatabaseContext().equals("twitter")){
//				bkgValue = TwitterFollowerCollector.getUserFollowerFromDB(evaluationTime, temp.userId);
//			}
//			if(Config.INSTANCE.getDatabaseContext().equals("stock")){
//				bkgValue = RemoteBKGManager.INSTANCE.getCurrentStockRevenueFromDB(evaluationTime, temp.userId);
//			}
			int bkgValue=TwitterFollowerCollector.getUserFollowerFromDB(evaluationTime, temp.userId);
			if(replicaValue==bkgValue)
				{
				result.put(temp.userId,"=");
				}
			else 
				{
				result.put(temp.userId, "<>" + replicaValue + "  " + bkgValue);
				minTopK.addFollowerReplica (temp.userId , bkgValue );
				}
			counter++;
		}
		return result;
	}
	
	

}
