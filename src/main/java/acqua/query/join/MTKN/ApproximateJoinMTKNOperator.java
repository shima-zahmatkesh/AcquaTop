package acqua.query.join.MTKN;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import acqua.config.Config;
import acqua.data.RemoteBKGManager;
import acqua.data.TwitterFollowerCollector;
import acqua.maintenance.MinTopK;

public abstract class ApproximateJoinMTKNOperator {


	protected HashMap<Long, Integer> followerReplica;
	protected HashMap<Long,Long> 
	userInfoUpdateTime, //the time on which the item was updated 
	estimatedLastChangeTime, //the time on which the item was updated and changed
	bkgLastChangeTime;  //the time on which the item was updated and changed (readed by bkg)
	protected HashMap<Long,Integer> freshFollowerCount;// = TwitterFollowerCollector.getFollowerListFromDB(timeStamp);

	protected FileWriter answersFileWriter;
	protected FileWriter selectedCondidatesFileWriter; 
	protected FileWriter statsFileWriter;
	
	protected MinTopK minTopK = new MinTopK();
	
	public ApproximateJoinMTKNOperator(){	
		
		HashMap<Long,String> userInfo=new HashMap<Long, String>();
		followerReplica=new HashMap<Long, Integer>();
		userInfoUpdateTime = new HashMap<Long, Long>();
		estimatedLastChangeTime = new HashMap<Long, Long>();
		bkgLastChangeTime = new HashMap<Long, Long>();
		
		if( Config.INSTANCE.getDatabaseContext().equals("twitter") ){
			userInfo = TwitterFollowerCollector.getInitialUserFollowersFromDB(); // ==>  firstWindow
		}
		if(Config.INSTANCE.getDatabaseContext().equals("stock")){
			userInfo = RemoteBKGManager.INSTANCE.getInitialBkgInfoFromDB(); // initial information(revenue) of stocks
		}
		
		
		
		
		Iterator<Long> it = userInfo.keySet().iterator();
		while(it.hasNext()){
			Long temp=it.next();
			String fcT = userInfo.get(temp);
			String[] followerTime =  fcT.split(",");
			followerReplica.put(temp, Integer.parseInt(followerTime[0]));
			userInfoUpdateTime.put(temp, Long.parseLong(followerTime[1]));//follower info is according to the end of first window
			estimatedLastChangeTime.put(temp, Long.parseLong(followerTime[1]));//follower info is according to the end of first window
			bkgLastChangeTime.put(temp, Long.parseLong(followerTime[1]));
		}

		try{
			answersFileWriter = new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/"+this.getClass().getSimpleName()+"Output.txt"));
			selectedCondidatesFileWriter= new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"Debug/"+this.getClass().getSimpleName()+"selectedupdateEntries.txt"));
			statsFileWriter= new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"Debug/"+this.getClass().getSimpleName()+"estimationErrorPerWindow.txt"));
			statsFileWriter.write("p, s, p&s, totalNumberOfCandidatesinML, numberOfExpiredCandidatesinML, numberOfExpiredCandidatesAfterTheMaintenanceinML, numberOfExpiredElementsInTheView, numberOfExpiredElementsInTheViewAfterTheMaintenance \n");
		}catch(Exception e){
			e.printStackTrace();
		}
			
	}

	public void populateMTKN(ArrayList<HashMap<Long, Integer>>  slidedwindows, ArrayList<HashMap<Long, Long>> slidedwindowsTime){
	
		minTopK.populateSliededWindows(slidedwindows);
		minTopK.populateSliededWindowsTime(slidedwindowsTime);
		minTopK.populateFollowerReplica(followerReplica);
	}
	
	public void populateMTKNCurrentWindow(HashMap<Long, Integer>  slidedwindow, HashMap<Long, Long> slidedwindowTime){
		
		minTopK.populateSliededWindow(slidedwindow);
		minTopK.populateSliededWindowTime(slidedwindowTime);
		minTopK.populateFollowerReplica(followerReplica);
	}
	
	public void process(long evaluationTime, Map<Long,Integer> mentionList,Map<Long,Long> usersTimeStampOfTheCurrentSlidedWindow){			
		try {
			//skip the first iteration
			long windowDiff = evaluationTime-Config.INSTANCE.getQueryStartingTime();
			if (windowDiff==0) 
				return;
			
			//for statistics
			freshFollowerCount = TwitterFollowerCollector.getFollowerListFromDB(evaluationTime);
			Double E = computeWindowError(mentionList,evaluationTime);
			Double E_b = computeReplicaError(evaluationTime );

			// MTKN algorithm
			
			//perform MTKN to get the top-k query results
			TreeMap<Long,Integer> currentCandidate = new TreeMap<Long,Integer>();
			TreeMap<Long,Long> currentCandidateTime = new TreeMap<Long,Long>();
			TreeMap<Long,Integer> currentChanges = new TreeMap<Long,Integer>();
			
			currentCandidate.putAll(mentionList);
			currentCandidateTime.putAll(usersTimeStampOfTheCurrentSlidedWindow);
			
			//System.out.println("MTKN before process currnt window");
			//MinTopK.data.printMTKN();
			
			minTopK.processCurrentWindowForNewArrival(currentCandidate , currentCandidateTime ,  currentChanges);
			
			//System.out.println("MTKN after process currnt window");
			//MinTopK.data.printMTKN();
			
			
			//invoke FollowerTable::getFollowers(user,ts) and updates the replica for a subset of users that exist in stream
			HashMap<Long,String> electedElements = updatePolicy(mentionList.keySet().iterator(),usersTimeStampOfTheCurrentSlidedWindow, evaluationTime);
			selectedCondidatesFileWriter.write(electedElements.toString() + "," + evaluationTime + "\n");
		
		
			//System.out.println("--------------evaluation time  = "+ evaluationTime );
			//update the users
			for(long id : electedElements.keySet()){
				
				//read the new value (= invoke the remote service)
				Integer newValue = 0;
				
				newValue = TwitterFollowerCollector.getUserFollowerFromDB(evaluationTime, id);
				
//				if(Config.INSTANCE.getDatabaseContext().equals("twitter")){
//					newValue = TwitterFollowerCollector.getUserFollowerFromDB(evaluationTime, id);
//				}
//				if(Config.INSTANCE.getDatabaseContext().equals("stock")){
//					newValue = RemoteBKGManager.INSTANCE.getCurrentStockRevenueFromDB(evaluationTime, id);
//				}
	
				Integer oldValue = followerReplica.get(id);
				
				if(!oldValue.equals(newValue)){
					followerReplica.put(id,newValue);
					estimatedLastChangeTime.put(id, evaluationTime);
					currentChanges.put(id, newValue);
					//System.out.println("add user id = "+ id + " to the current changes with value = " + newValue + " - the old value = " + oldValue);
				} else {
					
				}
				//bkgLastChangeTime.put(id, TwitterFollowerCollector.getPreviousExpTime(id,evaluationTime));
				userInfoUpdateTime.put(id, evaluationTime);
			}
			
			//for statistics
			Double EP = computeWindowError( mentionList,evaluationTime);
			Double EP_b = computeReplicaError(evaluationTime);
			statsFileWriter.write(","+ mentionList.size()+","+ E +","+ EP +","+ E_b +","+ EP_b +" \n");
		
		
			minTopK.processCurrentWindowForBKGChanges(currentCandidate , currentCandidateTime ,  currentChanges);
			
			ArrayList<String> topKResult =minTopK.getTopKResult() ;
				int rank = 1;
				Iterator<String> it= topKResult.iterator();
				while(it.hasNext()){
				
					String temp = it.next();
					String[] splitTemp = temp.split(",");
					long userId = Long.parseLong(splitTemp[0]);
					float score = Float.parseFloat(splitTemp[1]);
					Integer userFollowers = followerReplica.get(userId);
					if (userFollowers == null) userFollowers =0;
					if (currentCandidate.get(userId)== null) 
						System.out.println("----------------------------- ERRRRROR ---------------------" +userId);
					
					answersFileWriter.write(userId +" "+ currentCandidate.get(userId) +" "+ userFollowers +" "+ evaluationTime +  " " + score + " " + rank +"\n");
					rank ++;
				}
				
				currentCandidate.clear();
				currentCandidateTime.clear();
				
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private double computeWindowError(Map<Long,Integer> mentionList,long evaluationTime){
		double error=0;
		Iterator<Long> windowContent = mentionList.keySet().iterator();
		while(windowContent.hasNext())	
		{
			Long userId=windowContent.next();
			if(freshFollowerCount.get(userId)==null || freshFollowerCount.get(userId).intValue()!=followerReplica.get(userId).intValue())
				error+=1;
		}
		return error;
	}

	private double computeReplicaError(long evaluationTime){
		double error=0;
		Iterator<Long> allUserCountIt = followerReplica.keySet().iterator();
		while(allUserCountIt.hasNext()){
			Long userId=allUserCountIt.next();
			if(freshFollowerCount.get(userId)==null || freshFollowerCount.get(userId).intValue()!=followerReplica.get(userId).intValue())
				error+=1;
		}
		return error;
	}

	public void close(){
		try{
			answersFileWriter.flush();
			answersFileWriter.close();
			selectedCondidatesFileWriter.flush();
			selectedCondidatesFileWriter.close();
			statsFileWriter.flush();
			statsFileWriter.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	protected boolean isStale(long timestamp, long id){

//		int newValue =0;
//		
//		if(Config.INSTANCE.getDatabaseContext().equals("twitter")){
//			newValue = TwitterFollowerCollector.getUserFollowerFromDB(timestamp, id);
//		}
//		if(Config.INSTANCE.getDatabaseContext().equals("stock")){
//			newValue = RemoteBKGManager.INSTANCE.getCurrentStockRevenueFromDB(timestamp, id);
//		}
		
		int newValue = TwitterFollowerCollector.getUserFollowerFromDB(timestamp, id);
		
		if(followerReplica.get(id).equals(newValue))
			return false;
		return true;
	}
	
	protected abstract HashMap<Long,String> updatePolicy(Iterator<Long> candidateUserSetIterator,Map<Long,Long> usersTimeStampOfTheCurrentSlidedWindow, long evaluationTime );
	
	public void outputTopKResult() {
		minTopK.outputTopKResult();
	}
		
	public void purgeExpiredWindow(int window){
		minTopK.purgeExpiredWindow(window);
	}

	public void setCurrentWindow(int window) {
		minTopK.setCurrentWindow(window);
		
	}

	
}
