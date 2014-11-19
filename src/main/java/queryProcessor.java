import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;


public class queryProcessor {
	JoinOperator join;
	twitterStreamCollector tsc;
	TwitterFollowerCollector tfc;
	HashMap<Long, Integer> initialCache;
	public static long start=new Long("1416244704221");
	public static int WindowSize=30;
	public queryProcessor(){
		tsc= new twitterStreamCollector();
		tsc.extractWindow(WindowSize, "D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/twitterStream.txt");
		//tsc.windows.get(1);  ==>  firstWindow
		tfc=new TwitterFollowerCollector();				
		initialCache = tfc.getFollowerListFromDB(start); //gets the first window
	}
	public void evaluateQuery(long timeStamp){
		join.process(timeStamp);
	}
	public interface JoinOperator{
		public void process(long timeStamp);
	}
	//----------------------------------------------------------------------------------------
	public class DWJoinOperator implements JoinOperator{
		public void process(long timeStamp){
			long windowDiff = timeStamp-start;
			int index=((int)windowDiff)/WindowSize;			
			HashMap<Long,Integer> MentionList = tsc.windows.get(index);
			//we join mentionList with initial cache and return result
			Iterator it= MentionList.keySet().iterator();
			while(it.hasNext()){
				long userId=Long.parseLong(it.next().toString());
				Integer userFollowers = initialCache.get(userId);
				System.out.println(userId +" "+MentionList.get(userId)+" "+userFollowers);
			}
			
		}
	}
	//----------------------------------------------------------------------------------------
	public class OracleJoinOperator implements JoinOperator{
		public void process(long timeStamp){
			HashMap<Long, Integer> currentFollowerCount=
			long windowDiff = timeStamp-start;
			int index=((int)windowDiff)/WindowSize;			
			HashMap<Long,Integer> MentionList = tsc.windows.get(index);
			//we join mentionList with initial cache and return result
			Iterator it= MentionList.keySet().iterator();
			while(it.hasNext()){
				long userId=Long.parseLong(it.next().toString());
				Integer userFollowers = initialCache.get(userId);
				System.out.println(userId +" "+MentionList.get(userId)+" "+userFollowers);
			}
			
		}
	}
	//----------------------------------------------------------------------------------------
	public abstract class ApproximateJoinOperator implements JoinOperator{
		protected Map<long,int> replica;
		protected int updateBudget;

		public void process(long timestamp){
			for(long id : updatePolicy()){
				//invoke FollowerTable::getFollowers(user,ts) and updates the replica
			}
			//process the join
		}
		protected abstract Set<long> updatePolicy();
	}
	
	public class BaselineJoinOperator extends ApproximateJoinOperator{
		protected Set<long> updatePolicy(){
		//decide which rows to update and return the list
		//it must satisfy the updateBudget constraint!
		}
	}
	public static void main(String[] args){
	
	}
}
