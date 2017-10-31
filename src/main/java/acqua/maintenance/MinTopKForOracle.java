package acqua.maintenance;

import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;

public class MinTopKForOracle extends MinTopK{

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
			long beginWindow = beginOfcurrentWindow();
			long endWindow = endOfCurrentWindow();
			if (  time >= beginWindow &&  time < endWindow){ //|| time >= endOfPreviouseWindow()){
				Node newNode = new Node();
				newNode.setScore(ScoringFunction.computeScore(objectId, followerNum, mentionNum));
				newNode.setObjectId(objectId);
				newNode.setTime(time);

				if (data.getCurrentWindow() != data.getInitialWindow() && time < endOfPreviouseWindow()){
				//	System.out.println("ariving  existing node " + newNode.getObjectId() + "   at time " + time);
					data.updateMtkn(newNode);
				}else{
					data.updateMtkn(newNode);		// all the arriving node will add to the MTKN list if they have enough score  even the same user id which come in new window
				//	System.out.println("ariving node " + newNode.getObjectId() + "   at time " + time);
				}

				//data.printMTKN();
				//data.printLBP();
				//data.printActiveWindow();	
			}
		}
	}
		
}
