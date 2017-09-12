package acqua.maintenance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;


public class MinTopK {
	
	static private MaintenanceData data ;
	public ArrayList<TreeMap<Long,Integer>> slidedwindows;
	public ArrayList<TreeMap<Long,Long>> slidedwindowsTime;
	public ArrayList<TreeMap<Long,Integer>> Backgroundchanges;  // for each window, give an arrayList that contains the userId and the new value
	public  HashMap<Long, Integer> followerReplica;
	
	
	
	public MinTopK() {
		data = new MaintenanceData(3,2) ;
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
		
		// update mtkn based on new entery of the streaming part
		Iterator<Long> it = currentCandidate.keySet().iterator() ;
		while (it.hasNext()){
			long objectId = it.next();
			int mentionNum = currentCandidate.get(objectId);
			int followerNum = followerReplica.get(objectId);
			long time = currentCandidateTime.get(objectId);
			data.checkExpiredWindow(time);
			if ( data.getCurrentWindow() == data.getInitialWindow() || time >= endOfPreviouseWindow()){
				Node newNode = new Node();
				newNode.setScore(ScoringFunction.computeScore(objectId, followerNum, mentionNum));
				newNode.setObjectId(objectId);
				newNode.setTime(time);
				data.updateMtkn(newNode);
				System.out.println("\nariving node " + newNode.getObjectId());
				data.printMTKN();
				data.printLBP();
				data.printActiveWindow();	
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
				System.out.println("\nchanging node " + newNode.getObjectId());
				data.printMTKN();
				data.printLBP();
				data.printActiveWindow();
			}
		}
		
	//	data.generateCurrentWindowLBP();
	}


	private long endOfPreviouseWindow() {
		long time = data.getInitialTime()+ ((data.getCurrentWindow()-1) * data.getSlideTime()) + data.getWindowTime() ;
		return time;
	}
	
	private long endOfCurrentWindow() {
		long time = data.getInitialTime()+ ((data.getCurrentWindow()) * data.getSlideTime()) + data.getWindowTime() ;
		return time;
	}
	
	private long beginOfcurrentWindow() {
		long time = data.getInitialTime()+ ((data.getCurrentWindow()) * data.getSlideTime())  ;
		return time;
	}
	
	

	// print top-k resulst
	public void outputTopKResult() {
			
		data.outputTopKResult();
	}
		
	// print mtkn list
	public void printMTKN() {
		data.printMTKN();
	}

}
