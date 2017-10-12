package acqua.query.join.MTKN;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import acqua.config.Config;
import acqua.data.TwitterFollowerCollector;
import acqua.maintenance.MinTopK;
import acqua.maintenance.MinTopKForOracle;

public class MTKNOracleJoinOperator extends ApproximateJoinMTKNOperator  {

	public  FileWriter answersFileWriter;
	protected MinTopKForOracle minTopK = new MinTopKForOracle();
	
	public MTKNOracleJoinOperator() {
		
		followerReplica=new HashMap<Long, Integer>();
		try{
			String path= Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/"+this.getClass().getSimpleName()+"Output.txt";
			answersFileWriter = new FileWriter(new File(path));
		}catch(Exception e){e.printStackTrace();}
	}

	public void process(long evaluationTime, Map<Long,Integer> mentionList,Map<Long,Long> usersTimeStampOfTheCurrentSlidedWindow){			
		
		try {
			//skip the first iteration
			long windowDiff = evaluationTime-Config.INSTANCE.getQueryStartingTime();
			if (windowDiff==0) 
				return;

			HashMap<Long, Integer> currentFollowerCount=TwitterFollowerCollector.getFollowerListFromDB(evaluationTime);
			
			// MTKN algorithm
			
			TreeMap<Long,Integer> currentCandidate = new TreeMap<Long,Integer>();
			TreeMap<Long,Long> currentCandidateTime = new TreeMap<Long,Long>();
			TreeMap<Long,Integer> currentCahanges = new TreeMap<Long,Integer>();
			
			currentCandidate.putAll(mentionList);
			currentCandidateTime.putAll(usersTimeStampOfTheCurrentSlidedWindow);
			
			minTopK.populateFollowerReplica(currentFollowerCount);
			
			minTopK.processCurrentWindow(currentCandidate, currentCandidateTime ,currentCahanges);
			
			ArrayList<String> topKResult =minTopK.getTopKResult() ;
			
			int rank = 1;
			Iterator<String> it= topKResult.iterator();
			while(it.hasNext()){
				
				String temp = it.next();
				String[] splitTemp = temp.split(",");
				long userId = Long.parseLong(splitTemp[0]);
				float score = Float.parseFloat(splitTemp[1]);
				Integer userFollowers = currentFollowerCount.get(userId);  //TODO not contains the correct value
				if (userFollowers == null) userFollowers =0;
				
				answersFileWriter.write(userId +" "+ mentionList.get(userId) +" "+ userFollowers +" "+ evaluationTime +  " " + score + " " + rank +"\n");
				rank ++;
			}
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	public void close(){
		try{
			answersFileWriter.flush();
			answersFileWriter.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	@Override
	protected HashMap<Long, String> updatePolicy(Iterator<Long> CandidateIds,
			Map<Long, Long> candidateUserSetIterator, long evaluationTime) {
		// TODO Auto-generated method stub
		return null;
	}
}
