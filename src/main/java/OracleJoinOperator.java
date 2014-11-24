import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;


public class OracleJoinOperator implements JoinOperator{
		public  FileWriter J;
		public OracleJoinOperator(){
			try{
			J = new FileWriter(new File("D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/joinOutput/"+this.getClass().getSimpleName()+"Output.txt"));
			}catch(Exception e){e.printStackTrace();}
			
		}
		public void process(long timeStamp,HashMap<Long,Integer> mentionList){
			try {
				
			HashMap<Long, Integer> currentFollowerCount=TwitterFollowerCollector.getFollowerListFromDB(timeStamp);
			long windowDiff = timeStamp-queryProcessor.start;
			if (windowDiff==0) return;
			int index=((int)windowDiff)/(queryProcessor.windowSize*1000);			
			//HashMap<Long,Integer> mentionList = tsc.windows.get(index);
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