package acqua.query.join.acqua;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import acqua.config.Config;
import acqua.data.TwitterFollowerCollector;
import acqua.query.join.JoinOperator;

public class OracleJoinOperator implements JoinOperator{
	public  FileWriter outputWriter;
	public OracleJoinOperator(){
		try{
			String path= Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/"+this.getClass().getSimpleName()+"Output.txt";
			outputWriter = new FileWriter(new File(path));
		}catch(Exception e){e.printStackTrace();}

	}
	public void process(long timeStamp, Map<Long,Integer> mentionList,Map<Long,Long> usersTimeStampOfTheCurrentSlidedWindow){
		try {

			HashMap<Long, Integer> currentFollowerCount=TwitterFollowerCollector.getFollowerListFromDB(timeStamp);
			
			//FIXME: remove the dependency to QueryProcessor
			long windowDiff = timeStamp-Config.INSTANCE.getQueryStartingTime();
			if (windowDiff==0) return;
			int index=((int)windowDiff)/(Config.INSTANCE.getQueryWindowWidth()*1000);			
			//HashMap<Long,Integer> mentionList = tsc.windows.get(index);
			//we join current window of mentionList with initial cache and return result
			
			
			
			Iterator<Long> it= mentionList.keySet().iterator();
			while(it.hasNext()){
				long userId=Long.parseLong(it.next().toString());
				Integer userFollowers = currentFollowerCount.get(userId);
				
				//query contains filtering part
				if ( Config.INSTANCE.getQueryWithFiltering() ){
					if (userFollowers > Config.INSTANCE.getQueryFilterThreshold()){
						outputWriter.write(userId +" "+mentionList.get(userId)+" "+userFollowers+" "+timeStamp+"\n");
					}
				}
				//query without filtering part
				else{
					outputWriter.write(userId +" "+mentionList.get(userId)+" "+userFollowers+" "+timeStamp+"\n");
				}	
			}
			

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	

	
	
	
	public void close(){try{outputWriter.flush();outputWriter.close();}catch(Exception e ){e.printStackTrace();}}
}