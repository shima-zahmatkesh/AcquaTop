package acqua.query.join;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import acqua.config.Config;
import acqua.data.TwitterFollowerCollector;
import acqua.query.QueryProcessor;


public abstract class ApproximateDoubleJoinOperator implements JoinOperator {
	protected HashMap<Long, Integer> followerReplica;
	protected HashMap<Long,Integer>  statusCountReplica;
	protected HashMap<Long,Long> FollowerUpdateTime;
	protected HashMap<Long,Long> StatusCountUpdateTime;
	public  FileWriter J;
	 protected abstract  HashMap<Long,Integer> updatePolicyFollowerStatusCount(Iterator<Long> candidateUserSetIterator);
	 protected long timeStamp;
	 
	public ApproximateDoubleJoinOperator(){
		followerReplica=new HashMap<Long, Integer>();
		FollowerUpdateTime = new HashMap<Long, Long>();
		statusCountReplica = new HashMap<Long, Integer>();
		StatusCountUpdateTime = new HashMap<Long, Long>();
		followerReplica = TwitterFollowerCollector.getInitialUserFollowersFromDB(); // ==>  firstWindow
		statusCountReplica = TwitterFollowerCollector.getInitialUserStatusCountFromDB();
		Iterator it = followerReplica.keySet().iterator();
		while(it.hasNext()){
			FollowerUpdateTime.put(Long.parseLong(it.next().toString()),Config.INSTANCE.getQueryStartingTime());//follower info is according to the end of first window
		}
		it=statusCountReplica.keySet().iterator();
		while(it.hasNext()){
			StatusCountUpdateTime.put(Long.parseLong(it.next().toString()),Config.INSTANCE.getQueryStartingTime());//follower info is according to the end of first window
		}
		try{
		J = new FileWriter(new File("D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/joinOutput/"+this.getClass().getSimpleName()+"Output.txt"));
		}catch(Exception e){e.printStackTrace();}
	}
	public void process(long timeStamp,Map<Long,Integer> mentionList){			
		try {
		this.timeStamp=timeStamp;
		//process the join			
		long windowDiff = timeStamp-Config.INSTANCE.getQueryStartingTime();
		if (windowDiff==0) return;
		int index=((int)windowDiff)/(Config.INSTANCE.getQueryWindowWidth()*1000);			
		//HashMap<Long,Integer> mentionList = tsc.windows.get(index);		
		//invoke FollowerTable::getFollowers(user,ts) and updates the replica for a subset of users that exist in stream
		HashMap<Long,Integer> updated=updatePolicyFollowerStatusCount(mentionList.keySet().iterator());
		Iterator<Long> it=updated.keySet().iterator();
		while(it.hasNext()){
			long tempUpdateUserId=it.next();
			if(updated.get(tempUpdateUserId)==1){
				followerReplica.put(tempUpdateUserId,TwitterFollowerCollector.getUserFollowerFromDB(timeStamp, tempUpdateUserId));
				FollowerUpdateTime.put(tempUpdateUserId, timeStamp);
			}
			if(updated.get(tempUpdateUserId)==2){
				statusCountReplica.put(tempUpdateUserId,TwitterFollowerCollector.getUserFollowerFromDB(timeStamp, tempUpdateUserId));
				StatusCountUpdateTime.put(tempUpdateUserId, timeStamp);
			}
		}		
		//we join mentionList with current replica and return result	
		it= mentionList.keySet().iterator();
		while(it.hasNext()){
			long userId=Long.parseLong(it.next().toString());
			Integer userFollowers = followerReplica.get(userId);
			Integer StatusCount = statusCountReplica.get(userId);
			J.write(userId +" "+mentionList.get(userId)+" "+userFollowers+" "+StatusCount+" "+timeStamp+"\n");
		}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void close(){try{J.flush();J.close();}catch(Exception e){e.printStackTrace();}}
	
	
}
