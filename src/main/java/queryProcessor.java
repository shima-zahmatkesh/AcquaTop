import java.awt.image.ReplicateScaleFilter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.xerces.impl.xpath.regex.REUtil;


public class queryProcessor {
	JoinOperator join;
	twitterStreamCollector tsc;
	TwitterFollowerCollector tfc;
	protected HashMap<Long, Integer> followerReplica;
	protected HashMap<Long,Long> userInfoUpdateTime;
	protected int updateBudget;
	//HashMap<Long, Integer> initialCache;
	public static long start=1416244306470L;//select min(TIMESTAMP) + 30000 from BKG 
	public static int windowSize=30;
	public queryProcessor(int ub){//class JoinOperator){
		//updateBudget and join should be initiated
		updateBudget=ub;
		tfc=new TwitterFollowerCollector();				
		tsc= new twitterStreamCollector();
		tsc.extractWindow(windowSize, "D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/twitterStream.txt");
		followerReplica=new HashMap<Long, Integer>();
		userInfoUpdateTime = new HashMap<Long, Long>();
		followerReplica = tfc.getInitialUserFollowersFromDB(); // ==>  firstWindow
		Iterator it = followerReplica.keySet().iterator();
		while(it.hasNext()){
			userInfoUpdateTime.put(Long.parseLong(it.next().toString()),start);//follower info is according to the end of first window
		}
		//initialCache = tfc.getFollowerListFromDB(start); //gets the first window
	}
	public void evaluateQuery(long timeStamp){
		join.process(timeStamp);
	}
	public interface JoinOperator{
		public void process(long timeStamp);
	}
	
	//----------------------------------------------------------------------------------------
	public class OracleJoinOperator implements JoinOperator{
		public  FileWriter J;
		public OracleJoinOperator(){
			try{
			J = new FileWriter(new File("D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/joinOutput/"+this.getClass().getSimpleName()+"Output.txt"));
			}catch(Exception e){e.printStackTrace();}
			
		}
		public void process(long timeStamp){
			try {
				
			HashMap<Long, Integer> currentFollowerCount=tfc.getFollowerListFromDB(timeStamp);
			long windowDiff = timeStamp-start;
			if (windowDiff==0) return;
			int index=((int)windowDiff)/(windowSize*1000);			
			HashMap<Long,Integer> mentionList = tsc.windows.get(index);
			//we join current window of mentionList with initial cache and return result
			Iterator it= mentionList.keySet().iterator();
			while(it.hasNext()){
				long userId=Long.parseLong(it.next().toString());
				Integer userFollowers = currentFollowerCount.get(userId);
				J.write(userId +" "+mentionList.get(userId)+" "+userFollowers+" "+timeStamp+"\n");
			}
			
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		public void close(){try{J.flush();J.close();}catch(Exception e ){e.printStackTrace();}}
	}
	//----------------------------------------------------------------------------------------
	public abstract class ApproximateJoinOperator implements JoinOperator{
		public  FileWriter J;
		public ApproximateJoinOperator(){			
			try{
			J = new FileWriter(new File("D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/joinOutput/"+this.getClass().getSimpleName()+"Output.txt"));
			}catch(Exception e){e.printStackTrace();}
			
		}
		public void process(long timeStamp){			
			try {
			//process the join			
			long windowDiff = timeStamp-start;
			if (windowDiff==0) return;
			int index=((int)windowDiff)/(windowSize*1000);			
			HashMap<Long,Integer> mentionList = tsc.windows.get(index);		
			//invoke FollowerTable::getFollowers(user,ts) and updates the replica for a subset of users that exist in stream
			for(long id : updatePolicy(mentionList.keySet().iterator())){
				followerReplica.put(id,tfc.getUserFollowerFromDB(timeStamp, id));
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
	//----------------------------------------------------------------------------------------
		public class DWJoinOperator extends ApproximateJoinOperator{
			
			
			protected HashSet<Long> updatePolicy(Iterator<Long> candidateUserSetIterator){
				return new HashSet<Long>();
			}
		}
		//----------------------------------------------------------------------------------------
	public class BaselineJoinOperator extends ApproximateJoinOperator{
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
		            return (int)(o1.updateTimeDiff - o2.updateTimeDiff);
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
	public static void main(String[] args){
		queryProcessor qp=new queryProcessor(10);		
		BaselineJoinOperator bj=qp.new BaselineJoinOperator();
		OracleJoinOperator oj = qp.new OracleJoinOperator();
		DWJoinOperator dwj = qp.new DWJoinOperator();
		long time=queryProcessor.start;
		int windowCount=0;
		while(windowCount<30){
			time = time + 30000;
			//System.out.println(time +" "+windowCount+" "+queryProcessor.windowSize);
			oj.process(time);
			dwj.process(time);//why the error rate of DW doesn't show a constant increase? while in fact since the quality of information is decreasing during time, it must be a constant decrease
			//dwj and bj are sharing the same replica so changes over the replica of one affect changes over the replica of other
			bj.process(time);
			
			windowCount++;
		}
		bj.close();
		oj.close();
		dwj.close();
	}
}
