import java.awt.image.ReplicateScaleFilter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;


public class queryProcessor {
	JoinOperator join;
	twitterStreamCollector tsc;
	TwitterFollowerCollector tfc;
	//HashMap<Long, Integer> initialCache;
	public static long start=new Long("1416244704221");
	public static int windowSize=30;
	public queryProcessor(){
		tsc= new twitterStreamCollector();
		tsc.extractWindow(windowSize, "D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/twitterStream.txt");
		//tsc.windows.get(1);  ==>  firstWindow
		tfc=new TwitterFollowerCollector();				
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
		public void process(long timeStamp){
			FileWriter OracleJ;
			try {
				OracleJ = new FileWriter(new File("D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/joinOutput/DWJoinOutput.txt"));
			
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
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	//----------------------------------------------------------------------------------------
	public abstract class ApproximateJoinOperator implements JoinOperator{
		protected HashMap<Long, Integer> replica;
		protected int updateBudget;

		public void process(long timeStamp){
			
			FileWriter J;
			try {
				J = new FileWriter(new File("D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/joinOutput/"+this.getClass().toString()+"Output.txt"));
			
			//invoke FollowerTable::getFollowers(user,ts) and updates the replica
			for(long id : updatePolicy()){
				replica.put(id,tfc.getUserFollowerFromDB(timeStamp, id));
			}
			//process the join
			
			long windowDiff = timeStamp-start;
			int index=((int)windowDiff)/windowSize;			
			HashMap<Long,Integer> mentionList = tsc.windows.get(index);
			//we join mentionList with initial cache and return result
			Iterator it= mentionList.keySet().iterator();
			while(it.hasNext()){
				long userId=Long.parseLong(it.next().toString());
				Integer userFollowers = replica.get(userId);
				J.write(userId +" "+mentionList.get(userId)+" "+userFollowers+"\n");
			}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		protected abstract HashSet<Long> updatePolicy();
	}
	//----------------------------------------------------------------------------------------
		public class DWJoinOperator extends ApproximateJoinOperator{
			
			
			protected HashSet<Long> updatePolicy(){
				return new HashSet<Long>();
			}
		}
		//----------------------------------------------------------------------------------------
	public class BaselineJoinOperator extends ApproximateJoinOperator{
		protected HashSet<Long> updatePolicy(){
		//decide which rows to update and return the list
		//it must satisfy the updateBudget constraint!
			Iterator it = replica.keySet().iterator();
			while(it.hasNext()){
				Long userId = Long.parseLong(it.next().toString());
				replica.get(userId).
			}
		}
	}
	public static void main(String[] args){
	
	}
}
