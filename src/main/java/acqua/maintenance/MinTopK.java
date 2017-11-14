package acqua.maintenance;


import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.Map.Entry;
import acqua.config.Config;



public class MinTopK {
	
	static public MaintenanceData data ;
	public ArrayList<TreeMap<Long,Integer>> slidedwindows;
	public ArrayList<TreeMap<Long,Long>> slidedwindowsTime;
	public ArrayList<TreeMap<Long,Integer>> Backgroundchanges;  // for each window, give an arrayList that contains the userId and the new value
	public HashMap<Long, Integer> followerReplica;
	public static boolean DESC = false;
	public static boolean ASC = true;
	public int K = Config.INSTANCE.getK();
	public int N = Config.INSTANCE.getN();
	
	public MinTopK() {
		data = new MaintenanceData(K,N) ;
		followerReplica = new HashMap<Long, Integer>();
		Backgroundchanges = new ArrayList<TreeMap<Long,Integer>>();
		slidedwindows = new ArrayList<TreeMap<Long,Integer>>();
		slidedwindowsTime = new ArrayList<TreeMap<Long,Long>>();
	}

	public void evaluateQuery(){
		
		int window = data.getInitialWindow();
		while ( window < data.getMaxWindow()){

			TreeMap<Long,Integer> currentCandidate = slidedwindows.get(window);
			TreeMap<Long,Long> currentCandidateTime = slidedwindowsTime.get(window);
			TreeMap<Long,Integer> currentchanges = Backgroundchanges.get(window);
			
			data.setCurrentWindow(window);
			processCurrentWindow(currentCandidate ,currentCandidateTime ,currentchanges);
			data.outputTopKResult();
			data.purgeExpiredWindow(window);
			window++;
		}
		
	}
	        
	public void processCurrentWindow(TreeMap<Long,Integer> currentCandidate , TreeMap<Long,Long> currentCandidateTime , TreeMap<Long,Integer> currentChanges){
		
		HashMap <Long,Long> sortUserByTimestamp = sortByValue(currentCandidateTime , ASC);
		
		Iterator<Long> it = sortUserByTimestamp.keySet().iterator() ;
		while (it.hasNext()){
			long objectId = it.next();
			long time = sortUserByTimestamp.get(objectId);
			int followerNum = followerReplica.get(objectId);
			int mentionNum = currentCandidate.get(objectId);
			
			data.checkExpiredWindow(time);
			data.checkNewActiveWindow(time);
			
			//System.out.println("time " + time + "   endOfPreviouseWindow" + endOfPreviouseWindow() );
			
			if ( data.getCurrentWindow() == data.getInitialWindow()|| time >= endOfPreviouseWindow()){ // time < startOfNextWindow()){ //|| time >= endOfPreviouseWindow()){
				Node newNode = new Node();
				newNode.setScore(ScoringFunction.computeScore(objectId, followerNum, mentionNum));
				newNode.setObjectId(objectId);
				newNode.setTime(time);

				
				if (data.getCurrentWindow() != data.getInitialWindow() && time < endOfPreviouseWindow()){
					//System.out.println("ariving  existing node " + newNode.getObjectId() +" with  score" + newNode.getScore()+ "   at time " + time);
					data.updateMtkn(newNode);
				}else{
					data.updateMtkn(newNode);		// all the arriving node will add to the MTKN list if they have enough score  even the same user id which come in new window
					//System.out.println("ariving node " + newNode.getObjectId() +" with  score" + newNode.getScore()+ "   at time " + time);
				}
				data.printMTKN();
				//data.printLBP();
				//data.printActiveWindow();	
			}
		}
		// update mtkn based on changes of current window
		Iterator<Long> itch = currentChanges.keySet().iterator() ;
		while (itch.hasNext()){
			long objectId = itch.next();
			int mentionNum = currentCandidate.get(objectId);
			int followerNum = currentChanges.get(objectId);
			long time = currentCandidateTime.get(objectId);
			data.checkExpiredWindow(time);
			if ( time >= beginOfcurrentWindow() &&  time < endOfCurrentWindow()){
				Node newNode = new Node();
				newNode.setScore(ScoringFunction.computeScore(objectId, followerNum, mentionNum));
				newNode.setObjectId(objectId);
				newNode.setTime(time);
				data.updateMtkn(newNode);
				System.out.println("changing node " + newNode.getObjectId()+" with new score" + newNode.getScore() +  "   at time " + time);
				data.printMTKN();
				//data.printLBP();
				//data.printActiveWindow();
			}
		}
	
	}
	
	
	public void processCurrentWindowForNewArrival(TreeMap<Long,Integer> currentCandidate , TreeMap<Long,Long> currentCandidateTime , TreeMap<Long,Integer> currentChanges){
		
		HashMap <Long,Long> sortUserByTimestamp = sortByValue(currentCandidateTime , ASC);
		
		Iterator<Long> it = sortUserByTimestamp.keySet().iterator() ;
		while (it.hasNext()){
			long objectId = it.next();
			long time = sortUserByTimestamp.get(objectId);
			int followerNum = followerReplica.get(objectId);
			int mentionNum = currentCandidate.get(objectId);
			
			data.checkExpiredWindow(time);
			data.checkNewActiveWindow(time);
			
			//System.out.println("time " + time + "   endOfPreviouseWindow" + endOfPreviouseWindow() );
			
			if ( data.getCurrentWindow() == data.getInitialWindow()|| time >= endOfPreviouseWindow()){ // time < startOfNextWindow()){ //|| time >= endOfPreviouseWindow()){
				Node newNode = new Node();
				newNode.setScore(ScoringFunction.computeScore(objectId, followerNum, mentionNum));
				newNode.setObjectId(objectId);
				newNode.setTime(time);

				
				if (data.getCurrentWindow() != data.getInitialWindow() && time < endOfPreviouseWindow()){
					//System.out.println("ariving  existing node " + newNode.getObjectId() +" with  score" + newNode.getScore()+ "   at time " + time);
					data.updateMtkn(newNode);
				}else{
					data.updateMtkn(newNode);		// all the arriving node will add to the MTKN list if they have enough score  even the same user id which come in new window
					//System.out.println("ariving node " + newNode.getObjectId() +" with  score" + newNode.getScore()+ "   at time " + time);
				}
				data.printMTKN();
				//data.printLBP();
				//data.printActiveWindow();	
			}
		}
	//	System.out.println( " mtkn entry   :  " + data.getMTKNEntryOfCurrentWindow().toString());
	}
	public void processCurrentWindowForBKGChanges( TreeMap<Long,Integer> currentCandidate , TreeMap<Long,Long> currentCandidateTime , TreeMap<Long,Integer> currentChanges){

		// update mtkn based on changes of current window
		Iterator<Long> itch = currentChanges.keySet().iterator() ;
		while (itch.hasNext()){
			long objectId = itch.next();
			int mentionNum = currentCandidate.get(objectId);
			int followerNum = currentChanges.get(objectId);
			long time = currentCandidateTime.get(objectId);
			data.checkExpiredWindow(time);
			if ( time >= beginOfcurrentWindow() &&  time < endOfCurrentWindow()){
				Node newNode = new Node();
				newNode.setScore(ScoringFunction.computeScore(objectId, followerNum, mentionNum));
				newNode.setObjectId(objectId);
				newNode.setTime(time);
				data.updateMtkn(newNode);
				//System.out.println("changing node " + newNode.getObjectId()+" with new score" + newNode.getScore() +  "   at time " + time);
				//data.printMTKN();
				//data.printLBP();
				//data.printActiveWindow();
			}
		}
		//data.getMTKNEntryOfCurrentWindow().clear();
	
	}
	
//	private long startOfNextWindow() {
//		long time = data.getInitialTime()+ ((data.getCurrentWindow()+1) * data.getSlideTime()) ;
//		return time;
//	}
//		

	protected long endOfPreviouseWindow() {
		long time = data.getInitialTime()+ ((data.getCurrentWindow()-1) * data.getSlideTime()) + data.getWindowTime() ;
		return time;
	}
	
	protected long endOfCurrentWindow() {
		long time = data.getInitialTime()+ ((data.getCurrentWindow()) * data.getSlideTime()) + data.getWindowTime() ;
		return time;
	}
	
	protected long beginOfcurrentWindow() {
		long time = data.getInitialTime()+ ((data.getCurrentWindow()) * data.getSlideTime())  ;
		return time;
	}
	
	// return top-k results
	public ArrayList<String> getTopKResult() {
				
		return data.getTopKResult();
	}	
	
	// print top-k resulst
	public void outputTopKResult() {
			
		data.outputTopKResult();
	}
		
	// print mtkn list
	public void printMTKN() {
		data.printMTKN();
	}
	
	public void purgeExpiredWindow(int window){
		data.purgeExpiredWindow(window);
	}

	public void populateSliededWindows( ArrayList<HashMap<Long, Integer>> slidedWindow) {
		
		TreeMap<Long, Integer> temp = new TreeMap<Long, Integer>();
		for ( int i = 0 ; i < slidedWindow.size() ; i++){
			 
			Iterator <Long> it = slidedWindow.get(i).keySet().iterator();
			 while(it.hasNext()){
				 Long key = it.next();
				 Integer value = slidedWindow.get(i).get(key);
				 temp.put(key, value);		 
			 }
			 slidedwindows.add(temp);
		}
		
	}
	
	public void populateSliededWindowsTime( ArrayList<HashMap<Long, Long>> slidedWindowTime) {
		
		TreeMap<Long, Long> temp = new TreeMap<Long, Long>();
		for ( int i = 0 ; i < slidedWindowTime.size() ; i++){
			 
			Iterator <Long> it = slidedWindowTime.get(i).keySet().iterator();
			 while(it.hasNext()){
				 Long key = it.next();
				 Long value = slidedWindowTime.get(i).get(key);
				 temp.put(key, value);		 
			 }
			 slidedwindowsTime.add(temp);
		}
		
	}

	public void populateFollowerReplica(HashMap<Long, Integer> followerReplicaList) {
		
//		Iterator<Long> it = followerReplicaList.keySet().iterator();
//		while(it.hasNext()){
//			long key = it.next();
//			int	value = followerReplicaList.get(key);
//			followerReplica.put(key, value);
//		}
		followerReplica.putAll(followerReplicaList);	 
		
	}

	protected static HashMap<Long, Long> sortByValue (TreeMap<Long, Long> unsortMap, final boolean order)
	{

		List<Entry<Long, Long>> list = new LinkedList<Entry<Long, Long>>(unsortMap.entrySet());

		// Sorting the list based on values
		Collections.sort(list, new Comparator<Entry<Long, Long>>(){
			
			public int compare(Entry<Long, Long> o1,Entry<Long, Long> o2){
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
		HashMap<Long, Long> sortedMap = new LinkedHashMap<Long, Long>();
		for (Entry<Long, Long> entry : list)
		{
			sortedMap.put(entry.getKey(), entry.getValue());
		}

		return sortedMap;
	}

	public void setCurrentWindow(int window) {
		data.setCurrentWindow(window);
	}

	public ArrayList<String> getKMiddleResult() {

		return data.getKMiddleResult();
	}

	public ArrayList<String> getMTKNList() {
		return data.getMTKNList();
	}


}
