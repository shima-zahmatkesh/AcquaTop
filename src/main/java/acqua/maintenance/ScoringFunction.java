package acqua.maintenance;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


public class ScoringFunction {
	
	public static boolean DESC = false;
	public static float minMentions = Float.MAX_VALUE, minFollowerCount= Float.MAX_VALUE, maxMentions= Float.MIN_VALUE, maxFollowerCount= Float.MIN_VALUE , alpha = 0.9f;
	
	
	public static void setParam1Values (Map<Long, Integer> List){
		
		Iterator<Long> it= List.keySet().iterator();
		while(it.hasNext()){
			long key=it.next();
			Integer value = List.get(key);
			
			if (value >= maxMentions)
				maxMentions = value;
			
			if (value <= minMentions)
				minMentions = value;
		}
		
	}
	
	public static void setParam2Values (HashMap<Long, Integer> List){
		
		Iterator<Long> it= List.keySet().iterator();
		while(it.hasNext()){
			long key=it.next();
			Integer value = List.get(key);
			
			if (value >= maxFollowerCount)
				maxFollowerCount = value;
			
			if (value <= minFollowerCount)
				minFollowerCount = value;
		}
		
	}
	
	public static HashMap<Long, Float> getSortedUsers(Map<Long,Integer> mentionList , HashMap<Long, Integer> followerCount){
	
		HashMap<Long, Float> scoreOfUsers = new HashMap<Long, Float>();
		HashMap<Long, Float> sortedUser = new HashMap<Long, Float>();
		
		
		//setParam1Values (mentionList);
		//setParam2Values (followerCount);
		
		Iterator<Long> it= mentionList.keySet().iterator();
		while(it.hasNext()){
			long userId=Long.parseLong(it.next().toString());
			Integer userFollowers = followerCount.get(userId);
			Integer mentionNumber = mentionList.get(userId);
			
			Float score = computeScore(userId ,userFollowers, mentionNumber );
			scoreOfUsers.put(userId, score);
		}
		sortedUser = sortByValue(scoreOfUsers , DESC);
		return sortedUser;
	}

	public static Float computeScore(Long userId , Integer userFollowers, Integer mentionNumber ){
		
		//return (float)( ((float) userFollowers /100) +  mentionNumber);

		//return (float)(  alpha *( (userFollowers- minFollowerCount)/(maxFollowerCount-minFollowerCount) ) + (1-alpha)*( (mentionNumber- minMentions)/(maxMentions-minMentions) ) );
	
		return (float) userFollowers + (mentionNumber);
	}
	
	private  static HashMap<Long, Float> sortByValue (HashMap<Long, Float> unsortMap, final boolean order)
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

	public static void setMinMentions(float minMentions) {
		ScoringFunction.minMentions = minMentions;
	}

	public static void setMinFollowerCount(float minFollowerCount) {
		ScoringFunction.minFollowerCount = minFollowerCount;
	}

	public static void setMaxMentions(float maxMentions) {
		ScoringFunction.maxMentions = maxMentions;
	}

	public static void setMaxFollowerCount(float maxFollowerCount) {
		ScoringFunction.maxFollowerCount = maxFollowerCount;
	}
	
	
}
	

