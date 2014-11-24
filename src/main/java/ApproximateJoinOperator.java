import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;



public abstract class ApproximateJoinOperator implements JoinOperator{
	protected HashMap<Long, Integer> followerReplica;
	protected HashMap<Long,Long> userInfoUpdateTime;
	public  FileWriter J;
	public ApproximateJoinOperator(){	
		followerReplica=new HashMap<Long, Integer>();
		userInfoUpdateTime = new HashMap<Long, Long>();
		followerReplica = TwitterFollowerCollector.getInitialUserFollowersFromDB(); // ==>  firstWindow
		Iterator it = followerReplica.keySet().iterator();
		while(it.hasNext()){
			userInfoUpdateTime.put(Long.parseLong(it.next().toString()),queryProcessor.start);//follower info is according to the end of first window
		}
		try{
		J = new FileWriter(new File("D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/joinOutput/"+this.getClass().getSimpleName()+"Output.txt"));
		}catch(Exception e){e.printStackTrace();}
		
	}
	public void process(long timeStamp,HashMap<Long,Integer> mentionList){			
		try {
		//process the join			
		long windowDiff = timeStamp-queryProcessor.start;
		if (windowDiff==0) return;
		int index=((int)windowDiff)/(queryProcessor.windowSize*1000);			
		//HashMap<Long,Integer> mentionList = tsc.windows.get(index);		
		//invoke FollowerTable::getFollowers(user,ts) and updates the replica for a subset of users that exist in stream
		for(long id : updatePolicy(mentionList.keySet().iterator())){
			followerReplica.put(id,TwitterFollowerCollector.getUserFollowerFromDB(timeStamp, id));
			userInfoUpdateTime.put(id, timeStamp);
		}
		//we join mentionList with initial cache and return result	
		Iterator it= mentionList.keySet().iterator();
		while(it.hasNext()){
			long userId=Long.parseLong(it.next().toString());
			Integer userFollowers = followerReplica.get(userId);
			J.write(userId +" "+mentionList.get(userId)+" "+userFollowers+" "+timeStamp+"\n");
		}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void close(){try{J.flush();J.close();}catch(Exception e){e.printStackTrace();}}
	protected abstract HashSet<Long> updatePolicy(Iterator<Long> candidateUserSetIterator);
}
