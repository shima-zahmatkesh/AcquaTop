package acqua.query.join;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import acqua.config.Config;
import acqua.data.TwitterFollowerCollector;



public class OracleDoubleJoinOperator implements JoinOperator{
	public  FileWriter J;
	public OracleDoubleJoinOperator(){
		try{
			J = new FileWriter(new File("D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/joinOutput/"+this.getClass().getSimpleName()+"Output.txt"));
			}catch(Exception e){e.printStackTrace();}
	}
	public void process(long timeStamp, Map<Long, Integer> streamWindow) {
		try {
			HashMap<Long, Integer> currentFollowerCount=TwitterFollowerCollector.getFollowerListFromDB(timeStamp);
			HashMap<Long, Integer> currentStatusCount=TwitterFollowerCollector.getStsCountListFromDB(timeStamp);
			//process the join			
			//we join mentionList with current replica and return result	
			Iterator it= streamWindow.keySet().iterator();
			while(it.hasNext()){
				long userId=Long.parseLong(it.next().toString());
				Integer userFollowers = currentFollowerCount.get(userId);
				Integer StatusCount = currentStatusCount.get(userId);
				J.write(userId +" "+streamWindow.get(userId)+" "+userFollowers+" "+StatusCount+" "+timeStamp+"\n");
			}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
	}

	public void close() {
		try{J.flush();J.close();}catch(Exception e){e.printStackTrace();}		
	}

}
