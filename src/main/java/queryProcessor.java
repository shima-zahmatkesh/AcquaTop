import java.awt.image.ReplicateScaleFilter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;


public class queryProcessor {
	JoinOperator join;
	twitterStreamCollector tsc;
	TwitterFollowerCollector tfc;
	HashMap<Long, Integer> initialCache;
	public static long start=new Long("1416244704221");
	public static int windowSize=30;
	public queryProcessor(){
		tsc= new twitterStreamCollector();
		tsc.extractWindow(windowSize, "D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/twitterStream.txt");
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
	public class OracleJoinOperator implements JoinOperator{
		public void process(long timeStamp){
			FileWriter OracleJ=new FileWriter(new File("D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/joinOutput/DWJoinOutput.txt"));
			HashMap<Long, Integer> currentFollowerCount=tfc.getFollowerListFromDB(timeStamp);
			long windowDiff = timeStamp-start;
			int index=((int)windowDiff)/windowSize;			
			HashMap<Long,Integer> mentionList = tsc.windows.get(index);
			//we join mentionList with initial cache and return result
			Iterator it= mentionList.keySet().iterator();
			while(it.hasNext()){
				long userId=Long.parseLong(it.next().toString());
				Integer userFollowers = currentFollowerCount.get(userId);
				OracleJ.write(userId +" "+mentionList.get(userId)+" "+userFollowers+"\n");
			}			
		}
	}
	//----------------------------------------------------------------------------------------
	public abstract class ApproximateJoinOperator implements JoinOperator{
		protected HashMap<Long, Integer> replica;
		protected int updateBudget;

		public void process(long timestamp){
			for(long id : updatePolicy()){
				//invoke FollowerTable::getFollowers(user,ts) and updates the replica
			}
			//process the join
		}
		protected abstract Set<long> updatePolicy();
	}
	//----------------------------------------------------------------------------------------
		public class DWJoinOperator implements ApproximateJoinOperator{
			
			public void process(long timeStamp){
				
				FileWriter DWJ=new FileWriter(new File("D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/joinOutput/DWJoinOutput.txt"));
				
				long windowDiff = timeStamp-start;
				int index=((int)windowDiff)/windowSize;			
				HashMap<Long,Integer> mentionList = tsc.windows.get(index);
				//we join mentionList with initial cache and return result
				Iterator it= mentionList.keySet().iterator();
				while(it.hasNext()){
					long userId=Long.parseLong(it.next().toString());
					Integer userFollowers = replica.get(userId);
					DWJ.write(userId +" "+mentionList.get(userId)+" "+userFollowers+"\n");
				}
				
			}
			protected abstract Set<long> updatePolicy();
		}
		//----------------------------------------------------------------------------------------
	public class BaselineJoinOperator extends ApproximateJoinOperator{
		protected Set<long> updatePolicy(){
		//decide which rows to update and return the list
		//it must satisfy the updateBudget constraint!
			Iterator it = replica.keySet().iterator();
			while(it.hasNext()){
				
			}
		}
	}
	public static void main(String[] args){
	
	}
}
