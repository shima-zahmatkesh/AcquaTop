package acqua.query.join.topk;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import acqua.config.Config;
import acqua.data.TwitterFollowerCollector;
import acqua.query.join.JoinOperator;

public class TopKOracleJoinOperator implements JoinOperator{
	
	public  FileWriter outputWriter;
    public static boolean DESC = false;

    
	public TopKOracleJoinOperator(){
		try{
			String path= Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/"+this.getClass().getSimpleName()+"Output.txt";
			outputWriter = new FileWriter(new File(path));
		}catch(Exception e){e.printStackTrace();}

	}
	public void process(long timeStamp, Map<Long,Integer> mentionList,Map<Long,Long> usersTimeStampOfTheCurrentSlidedWindow){
		try {
			HashMap<Long, Float> scoreOfUsers = new HashMap<Long, Float>();
			HashMap<Long, Float> sortedUser = new HashMap<Long, Float>();
			HashMap<Long, Integer> currentFollowerCount=TwitterFollowerCollector.getFollowerListFromDB(timeStamp);
			
			long windowDiff = timeStamp-Config.INSTANCE.getQueryStartingTime();
			if (windowDiff==0) return;
			
			
			
			Iterator<Long> it= mentionList.keySet().iterator();
			while(it.hasNext()){
				long userId=Long.parseLong(it.next().toString());
				Integer userFollowers = currentFollowerCount.get(userId);
				Integer mentionNumber = mentionList.get(userId);
				
				Float score = computeScore(userId ,userFollowers, mentionNumber );
				scoreOfUsers.put(userId, score);
			}
			sortedUser = sortByValue(scoreOfUsers , DESC);	
			
			Long topk = Config.INSTANCE.getTopK();
			Iterator<Long> sortIt= sortedUser.keySet().iterator();
			while(sortIt.hasNext() && topk > 0 ){
				
				long userId=Long.parseLong(sortIt.next().toString());
				Integer userFollowers = currentFollowerCount.get(userId);
				
				outputWriter.write( userId + " " + mentionList.get(userId) + " " + userFollowers + " " + timeStamp + " " + sortedUser.get(userId)+ "\n");
				topk--;
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public Float computeScore(Long userId , Integer userFollowers, Integer mentionNumber ){
	
		return (float)(userFollowers + mentionNumber);
	}
	
	public  HashMap<Long, Float> sortByValue (HashMap<Long, Float> unsortMap, final boolean order)
	{

		List<Entry<Long, Float>> list = new LinkedList<Entry<Long, Float>>(unsortMap.entrySet());

		// Sorting the list based on values
		Collections.sort(list, new Comparator<Entry<Long, Float>>(){
			
			public int compare(Entry<Long, Float> o1,Entry<Long, Float> o2){
				if (order)
				{
					return o1.getValue().compareTo(o2.getValue());
				}
				else
				{
					return o2.getValue().compareTo(o1.getValue());

				}
			}
		});

		// Maintaining insertion order with the help of LinkedList
		HashMap<Long, Float> sortedMap = new LinkedHashMap<Long, Float>();
		for (Entry<Long, Float> entry : list)
		{
			sortedMap.put(entry.getKey(), entry.getValue());
		}

		return sortedMap;
	}

	
	public void close(){try{outputWriter.flush();outputWriter.close();}catch(Exception e ){e.printStackTrace();}}


}