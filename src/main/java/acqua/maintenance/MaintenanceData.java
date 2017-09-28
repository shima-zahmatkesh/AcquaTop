package acqua.maintenance;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import acqua.config.Config;




public class MaintenanceData {
	
	private  TreeMap< Key , Node >  mtkn = new TreeMap< Key, Node > ( new Comparator<Key>() { 

		public int compare(Key k1 , Key k2) {
			
			int res = (int) (k2.getScore() - k1.getScore());
			if(res!=0)
				return res;
			res=(int)(k2.getObjectId() - k1.getObjectId());   //res=(int)(k2.getTime() - k1.getTime());   TODO check for changing the tie breaker
				return res;    
		}
	});
	private TreeMap< Integer, Node> lbp = new TreeMap<Integer, Node>();    //LBP = Lower Bound Pointer for each Window  (number of the window map to the BNode of MTKN list ( Integer -> number of window, Node-> node in the MTKN list)
	private TreeMap <Integer , Integer > activeWindow = new TreeMap <Integer , Integer> (); //(Integer -> number of window , Integer -> top-k counter)

	private Node mtknLastNode = null;  //  TODO may change to first  key
	private int mtknSize ;  //( mtknSize = k + N)
	private int k ;
	private float minDistance = Float.MAX_VALUE;
	private int windowTime = Config.INSTANCE.getQueryWindowWidth() * 1000;
	private int slideTime = Config.INSTANCE.getQueryWindowSlide() * 1000;
	private int activeWindowNum = windowTime / slideTime ;
	private int currentWindow;
	private int maxWindow = Config.INSTANCE.getExperimentIterationNumber() + 3;
	private int initialWindow = 0;
	private long initialTime = Config.INSTANCE.getQueryStartingTime() ; // - 3 * Config.INSTANCE.getQueryWindowSlide() * 1000 ;
	
	
	public MaintenanceData(int k, int n) {
		super();
		this.mtknSize = k + n;
		this.k = k;
		this.currentWindow = initialWindow;
		//initialActiveWindow();
		//initialLbp();
		initializeMtkn();
		
	}

	// Update mtkn after expiration of a window
	public void purgeExpiredWindow(int expiredWindow){
	
		Iterator<Key> iter = mtkn.keySet().iterator();
		int index = 0;
		while ( iter.hasNext() && index < mtknSize && index < activeWindow.get(expiredWindow)) {
			Key key  = iter.next();
			Node node = mtkn.get(key);
				node.addStartWin();
			
			if (node.getStartWin() > node.getEndWin()){
				
				//update the lbp list id the deleted node has some pointers from lbp
				Key mtknKey = new Key( node.getObjectId() , node.getScore());
				Key k = mtkn.lowerKey(mtknKey);
				if ( k == null)
					k = mtkn.higherKey(mtknKey);
				for (Map.Entry<Integer, Node> TempNode : lbp.entrySet()) {
					if(TempNode.getValue().equals(node)){
						lbp.replace( TempNode.getKey() , mtkn.get( k ) );
					}
				}
				iter.remove();
			}
			index++;
		}
		removeFromActiveWindow(expiredWindow);
		lbp.remove(expiredWindow);
	}
	
	// remove a node from mtkn
	public void removeNodeFromMTKN(Node  node){
		
		if(lbp.containsValue(node)){
			
			Iterator <Integer> it = lbp.keySet().iterator();
			while (it.hasNext()){
				Integer key = it.next();
				if (lbp.get(key).equals(node))
					activeWindow.replace(key, activeWindow.get(key)-1);
			}
			lbp.remove(node);	
		
		} 

		for ( int i = node.getStartWin() ; i <= node.getEndWin() ; i++){
			activeWindow.replace(i, activeWindow.get(i)-1);
		}
		mtkn.remove(new Key(node.getObjectId() , node.getScore()));
		
	}
	
	//insert new node in mtkn
	public void insertNodeToMTKN(Node newNode){
		
		if ( fullResultForAllWindow() && ( mtknLastNode != null && newNode.getScore() < mtknLastNode.getScore()) )
			return;
		else{
			
			newNode.setEndWin(calculateEndWin(newNode.getTime()));
			newNode.setStartWin(calculateStartWin(newNode));
			
			if (mtkn.containsKey(new Key(newNode.getObjectId() , newNode.getScore()))){
				updateExistingNode(newNode);
			}
			else{
				mtkn.put(new Key(newNode.getObjectId() , newNode.getScore()), newNode);
				updateMinDistance(newNode);
				updateLbp(newNode);
			}
			
		}
		mtknLastNode = mtkn.get(mtkn.lastKey());

	}

	private void updateExistingNode(Node newNode) {
		
		Key newKey= new Key(newNode.getObjectId() , newNode.getScore());
		Node oldNode = mtkn.get(newKey);
		int oldEndWin = oldNode.getEndWin();
		
		mtkn.replace(newKey, newNode);
		
		//update LBP
		for ( int i = oldEndWin+1 ; i <= newNode.getEndWin(); i++){
			
			Node lbpNode = lbp.get(i);
			
			// if we dont have the win number in lbp, increase the activeWindow number and it necessary add it to the lbp
			if (lbpNode == null){
				activeWindow.replace(i, activeWindow.get(i)+1);
				if (activeWindow.get(i) == mtknSize)
					generateLBP(i);
				continue;
			}
			
			Key mtknKey = new Key( lbpNode.getObjectId() , lbpNode.getScore());
			Node mtknNode = mtkn.get(mtknKey);
				
				if ( lbpNode.getScore() <= newNode.getScore() ){
 					// number of top-k elements in window increase by 1 
					if(activeWindow.get(i) < mtknSize){
						activeWindow.replace(i, activeWindow.get(i)+1);
						if (activeWindow.get(i) == mtknSize)
							generateLBP(i);
						
					}
					// we have k elements in window, so increase the starting time one 
					else{
						mtknNode.setStartWin(mtknNode.getStartWin()+1);
						lbp.replace(i, mtknNode);
						mtkn.replace(mtknKey, mtknNode);
					}
					
					
					//remove node from mtkn list
					if (mtknNode.getStartWin() > mtknNode.getEndWin()){
						Key k = mtkn.lowerKey(mtknKey);
						if ( k == null)
							k = mtkn.higherKey(mtknKey);
						for (Map.Entry<Integer, Node> node : lbp.entrySet()) {
							if(node.getValue().equals(mtknNode)){
								lbp.replace( node.getKey() , mtkn.get( k ) );
							}
						}
						mtkn.remove(mtknKey);
					}	
				}
		}
		
	}

	// Update lbp info after inserting new node in mtkn
	private void updateLbp(Node newNode) {
		
		//update lbp
		Iterator<Integer> it = activeWindow.descendingKeySet().iterator();
		while( it.hasNext()){
			int key = it.next();
			Node lbpNode = lbp.get(key);
			
			if (key > newNode.getEndWin() || key < newNode.getStartWin())
				continue;
			
			if (lbpNode == null){
				activeWindow.replace(key, activeWindow.get(key)+1);
				if (activeWindow.get(key) == mtknSize)
					generateLBP(key);
				continue;
			}
			
			Key mtknKey = new Key( lbpNode.getObjectId() , lbpNode.getScore());
			Node mtknNode = mtkn.get(mtknKey);
				
				if ( lbpNode.getScore() <= newNode.getScore() ){
 					// number of top-k elements in window increase by 1 
					if(activeWindow.get(key) < mtknSize){
						activeWindow.replace(key, activeWindow.get(key)+1);
						if (activeWindow.get(key) == mtknSize)
							generateLBP(key);
						
					}
					// we have k elements in window, so increase the starting time one 
					else{
						mtknNode.setStartWin(mtknNode.getStartWin()+1);
						lbp.replace(key, mtknNode);
						mtkn.replace(mtknKey, mtknNode);
					}
					
					
					//remove node from mtkn list
					if (mtknNode.getStartWin() > mtknNode.getEndWin()){
						Key k = mtkn.lowerKey(mtknKey);
						if ( k == null)
							k = mtkn.higherKey(mtknKey);
						for (Map.Entry<Integer, Node> node : lbp.entrySet()) {
							if(node.getValue().equals(mtknNode)){
								lbp.replace( node.getKey() , mtkn.get( k ) );
							}
						}
						mtkn.remove(mtknKey);
					}
					
					
				}
		}
				
		for (Map.Entry<Integer, Node> entry : lbp.entrySet()) {
				
			if (entry.getKey() > entry.getValue().getEndWin() || entry.getKey() < entry.getValue().getStartWin()){
				Key mtknKey =  new Key (entry.getValue().getObjectId() , entry.getValue().getScore());
				Key k = mtkn.lowerKey(mtknKey);
				if ( k == null)
				k = mtkn.higherKey(mtknKey);
				lbp.replace( entry.getKey() , mtkn.get( k ) );
			}
		}
				
	}

	private boolean nodeIsInWindow(Node node, int window) {
		
		if ( node.getTime() >= ( window * slideTime) && node.getTime() < initialTime + ( window * slideTime)+ windowTime )
			return true;
		else
			return false;
	}

	// Update the MinDistance after inserting new node in mtkn
	private void updateMinDistance(Node newNode) {
		
		if (mtkn.ceilingKey(new Key( newNode.getObjectId() , newNode.getScore())) != null ){
			Node ceilingNode = mtkn.get(mtkn.ceilingKey(new Key( newNode.getObjectId() , newNode.getScore())));
			if ( ceilingNode.getScore() - newNode.getScore() < minDistance )
				minDistance = ceilingNode.getScore() - newNode.getScore();
		}
		
		if (mtkn.floorKey(new Key( newNode.getObjectId() , newNode.getScore()))!= null){
			Node floorNode = mtkn.get(mtkn.floorKey(new Key( newNode.getObjectId() , newNode.getScore())));
			if ( newNode.getScore() - floorNode.getScore() < minDistance)
				minDistance = newNode.getScore() -  floorNode.getScore();
		}
		
		
		
	}

	// Calculate the start window of the new inserting node
	private int calculateStartWin( Node calNode) {
		
		int result = -1;
		Node node = new Node();
		if (lbp.isEmpty()) result = initialWindow ;
		Iterator<Integer> it = lbp.keySet().iterator();
		
		while( it.hasNext()){
			int windowNum = it.next();
			node = lbp.get(windowNum);
			if ( node != null && node.getScore() <= calNode.getScore() && nodeIsInWindow (calNode , windowNum) ){
				result = windowNum;
				break;
			}
		}
		if (result == -1 ) result = calNode.getEndWin();
		return result;
	}
	
	// Calculate the end window of the new inserting node
	private int calculateEndWin( long time) {
			
		int result = -1;
		for ( int window = 0 ; window < Config.INSTANCE.getExperimentIterationNumber() +10 ;  window++){
				
			long startTimeWindow = (window * slideTime) + initialTime;
			long endTimeWindow = startTimeWindow + windowTime;
			
			if ( startTimeWindow <= time && time < endTimeWindow + windowTime &&  window > result )
				result = window;
				
		}
		if(!activeWindow.containsKey(result)){
			activeWindow.put(result, 0);
			System.out.println("Window " + result + " is added to the active windows - aendWin" + time);
		}
		return result;
	}
	
	// Check if all the windows has topK+N result or not
	private boolean fullResultForAllWindow(){
		
		boolean result = true;
		Iterator<Integer> it = activeWindow.keySet().iterator();
		while( it.hasNext()){
			Integer counter = activeWindow.get(it.next());
			if ( counter != mtknSize )
				result = false;		
		}
		return result;
	}
	
	// update the mtkn list based on the score changing of node 
	public void updateChangingObject( Node newNode , Node oldNode){
		
		if ( mtkn.containsKey(oldNode.getScore()) && mtkn.get(oldNode.getScore()).equals(oldNode)){  //TODO check if 2 aoject are the same
			
			if ( nodeCanChangeOrder(newNode , oldNode) ){
				removeNodeFromMTKN(oldNode);
				insertNodeToMTKN(newNode);
			}
			else{
				updateMinDistance(newNode);
			}
		}
		else{
			
			if (newNode.getScore() > mtknLastNode.getScore()){
				insertNodeToMTKN(newNode);
			}
		}
		mtknLastNode = mtkn.get(mtkn.lastKey());
			
	}

	// check if the changes in a node is grater than minDistance or not
	private boolean nodeCanChangeOrder(Node newNode, Node oldNode) {
		
		
		Key oldNodeKey = new Key( oldNode.getObjectId() , oldNode.getScore());
		Key lowerKey = mtkn.lowerKey(oldNodeKey);
		Key higherKey = mtkn.higherKey(oldNodeKey);
		Node lowerNode = null , higherNode = null;
		
		if (lowerKey != null) lowerNode = mtkn.get(lowerKey);
		if (higherKey != null)  higherNode = mtkn.get(higherKey);
		
		if ( lowerNode != null &&  higherNode!= null && ( newNode.getScore() > lowerNode.getScore() || newNode.getScore() < higherNode.getScore()) )
			return true;
		
		if ( lowerNode == null &&  newNode.getScore() < higherNode.getScore() )
			return true;
		
		if ( higherNode == null &&  newNode.getScore() > lowerNode.getScore() )
			return true;
		
		return false;
		//return ( Math.abs( newNode.getScore() - oldNode.getScore() ) > minDistance ) ;
		
	}

	// return top-k results
	public TreeMap <Long,Float> getTopKResult() {
					
		TreeMap <Long,Float> result = new TreeMap <Long,Float>();
		int index = 0;
		Iterator<Key> it = mtkn.keySet().iterator();
		while (it.hasNext() && index < k){
			Key key = it.next();
			result.put(key.getObjectId() , mtkn.get(key).getScore());
			index++;	
		}
		return result;	
	
	}		
	// print top-k results
	public void outputTopKResult() {
		
		int index = 0;
		Iterator<Key> it = mtkn.keySet().iterator();
		System.out.println("-------------------------------------------------------------------------------");

		System.out.println("Top-k results of window " + currentWindow);
		while (it.hasNext() && index < k){
			Key key = it.next();
			System.out.println("[" + key.getObjectId() + " , score=" + key.getScore() + " , sw=" + mtkn.get(key).getStartWin() + " , ew=" + mtkn.get(key).getEndWin() + " , t=" + mtkn.get(key).getTime() +"]");
			index++;
			
		}
		System.out.println("-------------------------------------------------------------------------------");

	}
	
	public void printMTKN() {
			
		Iterator<Key> it = mtkn.keySet().iterator();
		System.out.println("mtkn results:  size = " + mtkn.size());
		while (it.hasNext()){
			Key key = it.next();
			System.out.println("[" + key.getObjectId() + " , score=" + key.getScore() + " , sw=" + mtkn.get(key).getStartWin() + " , ew=" + mtkn.get(key).getEndWin() + " , t=" + mtkn.get(key).getTime() +"]");		
		}

	}

	public void printLBP(){
		
		String print = "lbp:";
		Iterator<Integer> it = lbp.keySet().iterator();
		
		while (it.hasNext()){
			int key = it.next();
			print = print.concat(" w" + key + ":"+ lbp.get(key).getObjectId() + " , "  );		
		}
		System.out.println(print);
	}
	
	public void printActiveWindow(){
		
		String print = "AW:";
		Iterator<Integer> it = activeWindow.keySet().iterator();
		
		while (it.hasNext()){
			int key = it.next();
			print = print.concat(" w" + key + ":"+ activeWindow.get(key) + " , "  );		
		}
		System.out.println(print + "\n");
	}
	
	public void updateMtkn(Node newNode) {

		insertNodeToMTKN(newNode);	
		mtknLastNode = mtkn.get(mtkn.lastKey());
		
	}
	
	public void updateMtknForChanges(Node newNode) {

		if ( mtknContainsNode(newNode)){
			
			Node oldNode = getOldNode( newNode);
			replaceNewNode(oldNode , newNode);
		}
		else{	
			insertNodeToMTKN(newNode);	
		}
		mtknLastNode = mtkn.get(mtkn.lastKey());
	}
	
	private void replaceNewNode(Node oldNode, Node newNode) {
		
		newNode.setStartWin(oldNode.getStartWin());
		newNode.setEndWin(oldNode.getEndWin());
		mtkn.remove(new Key(oldNode.getObjectId() , oldNode.getScore()));
		mtkn.put(new Key(newNode.getObjectId() , newNode.getScore()), newNode);
		refreshLbp();
		
	}

	private void refreshLbp() {

		TreeMap < Integer , Integer> TopKForWindow = new TreeMap < Integer , Integer>();
		Iterator<Integer> itlbp = lbp.keySet().iterator();
		while(itlbp.hasNext()){
			int key = itlbp.next();
			TopKForWindow.put(key, 0);	
		}
		
		
		Iterator<Key> it = mtkn.keySet().iterator();
		while(it.hasNext()){
			Key key = it.next();
			Node node = mtkn.get(key);
			for ( int i = node.getStartWin() ; i <= node.getEndWin() ; i++ ){
				TopKForWindow.replace(i, TopKForWindow.get(i)+1);
				if ( TopKForWindow.get(i) == mtknSize )
					lbp.replace(i, node);
			}	
		}
	}

	private void updateNode(Node oldNode, Node newNode) {
		
		newNode.setEndWin(calculateEndWin(newNode.getTime()));
		newNode.setStartWin(calculateStartWin(newNode));
		
		mtkn.remove(oldNode.getKey());
		mtkn.put(newNode.getKey(), newNode);	
		for (Map.Entry<Integer, Node> node : lbp.entrySet()) {
			if(node.getValue().equals(oldNode)){
				lbp.replace( node.getKey() , newNode );
			}
		}
		updateLbp(newNode);
	}

	private Node getOldNode(Node newNode) {
		
		Iterator<Key> iter = mtkn.keySet().iterator();
		while ( iter.hasNext()) {
			Key key  = iter.next();
			if( newNode.getObjectId() == key.getObjectId())
				return mtkn.get(key);
		}

		return null;
	}

	private boolean mtknContainsNode(Node newNode) {
		
		Iterator<Key> iter = mtkn.keySet().iterator();
		while ( iter.hasNext()) {
			Key key  = iter.next();
			if( newNode.getObjectId() == key.getObjectId())
				return true;
		}

		return false;
	}

	public int getWindowTime() {
		return windowTime;
	}

	public void setWindowTime(int windowTime) {
		this.windowTime = windowTime;
	}

	public int getSlideTime() {
		return slideTime;
	}

	public void setSlideTime(int slideTime) {
		this.slideTime = slideTime;
	}

	public long getInitialTime() {
		return initialTime;
	}

	public void setInitialTime(long initialTime) {
		this.initialTime = initialTime;
	}

	public void initialActiveWindow(){
		
		for ( int i = 0 ; i < activeWindowNum; i++)
		activeWindow.put(i, 0);	
	}

	public void initialLbp(){
		
		for ( int i = 0 ; i < activeWindowNum; i++)
		lbp.put(i, null);	
	}
	
	public void removeFromActiveWindow(int i){
		activeWindow.remove(i);	
	}
	
	public int getK() {
		return k;
	}

	public void setK(int k) {
		this.k = k;
	}

	public int getCurrentWindow() {
		return currentWindow;
	}

	public void setCurrentWindow(int currentWindow) {
		this.currentWindow = currentWindow;
	}

	public int getActiveWindowNum() {
		return activeWindowNum;
	}

	public void setActiveWindowNum(int activeWindowNum) {
		this.activeWindowNum = activeWindowNum;
	}

	public int getMaxWindow() {
		return maxWindow;
	}

	public void setMaxWindow(int maxWindow) {
		this.maxWindow = maxWindow;
	}
	
	public int getInitialWindow() {
		return initialWindow;
	}

	public void setInitialWindow(int initialWindow) {
		this.initialWindow = initialWindow;
	}

	public void initializeMtkn(){
		
		initialActiveWindow();
		
	}

	public void checkExpiredWindow(long time) {
		
		Iterator<Integer> it = activeWindow.keySet().iterator();  
		while( it.hasNext()){
			int win = it.next();
			long t = endOfWindow(win);
			if (time >= t)
				purgeExpiredWindow(win);
				break;
			}
	}
	
	private long endOfWindow(long window) {
		long time = (long) (getInitialTime()+ (window * getSlideTime()) + getWindowTime()) ;
		return time;
	}
	
	private long startOfWindow(int window) {
		long time = (long) ( getInitialTime() + (window * getSlideTime()) ) ;
		return time;
	}

	public void generateLBP(int window) {
		
		mtknLastNode = mtkn.get(mtkn.lastKey());
		if (!lbp.containsKey(window))
			lbp.put(window, mtknLastNode);
		
		if (window > mtknLastNode.getEndWin() || window < mtknLastNode.getStartWin()){
			Key mtknKey= new Key (mtknLastNode.getObjectId() , mtknLastNode.getScore()) ; 
			Key k = mtkn.lowerKey(mtknKey);
			if ( k == null)
			k = mtkn.higherKey(mtknKey);
			lbp.replace( window , mtkn.get( k ) );
		}
		
	}

	public void checkNewActiveWindow(long time) {
		
		//System.out.println("checkNewActiveWindow for time    " + time);
		for ( int i = 0 ; i < (int)( windowTime/slideTime) ; i++){

			int nextWindow = currentWindow + i + 1;
			long startOfNextWindow = startOfWindow(currentWindow) + ((i + 1) * slideTime ) ;
			//System.out.println(" check if   " + startOfNextWindow + "  <   " + time + " for next window equal to "+ nextWindow );
			
			if ( ( startOfNextWindow < time ) && !activeWindow.containsKey(nextWindow)){
				
				activeWindow.put(nextWindow, 0);
				System.out.println("Window " + nextWindow + " is added to the active windows");
			}
			
		}
		
	}

	

	
}
