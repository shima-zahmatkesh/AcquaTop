package acqua.query.join.acqua;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import acqua.config.Config;
import acqua.data.TwitterFollowerCollector;
import acqua.query.join.JoinOperator;
import acqua.query.join.topk.ScoringFunction;
import acqua.query.join.topk.TopKOracleJoinOperator;


public abstract class ApproximateJoinOperator implements JoinOperator{


	protected HashMap<Long, Integer> followerReplica;
	protected HashMap<Long,Long> 
	userInfoUpdateTime, //the time on which the item was updated 
	estimatedLastChangeTime, //the time on which the item was updated and changed
	bkgLastChangeTime;  //the time on which the item was updated and changed (readed by bkg)
	protected HashMap<Long,Integer> freshFollowerCount;// = TwitterFollowerCollector.getFollowerListFromDB(timeStamp);

	protected FileWriter answersFileWriter;
	protected FileWriter selectedCondidatesFileWriter; 
	protected FileWriter statsFileWriter;
	
	public ApproximateJoinOperator(){	
		HashMap<Long,String> userInfo=new HashMap<Long, String>();
		followerReplica=new HashMap<Long, Integer>();
		userInfoUpdateTime = new HashMap<Long, Long>();
		estimatedLastChangeTime = new HashMap<Long, Long>();
		bkgLastChangeTime = new HashMap<Long, Long>();

		userInfo = TwitterFollowerCollector.getInitialUserFollowersFromDB(); // ==>  firstWindow
		Iterator<Long> it = userInfo.keySet().iterator();

		while(it.hasNext()){
			Long temp=it.next();
			String fcT = userInfo.get(temp);
			String[] followerTime =  fcT.split(",");
			followerReplica.put(temp, Integer.parseInt(followerTime[0]));
			userInfoUpdateTime.put(temp, Long.parseLong(followerTime[1]));//follower info is according to the end of first window
			estimatedLastChangeTime.put(temp, Long.parseLong(followerTime[1]));//follower info is according to the end of first window
			bkgLastChangeTime.put(temp, Long.parseLong(followerTime[1]));
			
			//userInfoUpdateTime.put(temp, Config.INSTANCE.getQueryStartingTime());//follower info is according to the end of first window
			//estimatedLastChangeTime.put(temp, Config.INSTANCE.getQueryStartingTime());//follower info is according to the end of first window
			//bkgLastChangeTime.put(temp, Config.INSTANCE.getQueryStartingTime());
		}

		try{
			answersFileWriter = new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/"+this.getClass().getSimpleName()+getSuffix()+"Output.txt"));
			selectedCondidatesFileWriter= new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"Debug/"+this.getClass().getSimpleName()+getSuffix()+"selectedupdateEntries.txt"));
			statsFileWriter= new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"Debug/"+this.getClass().getSimpleName()+getSuffix()+"estimationErrorPerWindow.txt"));
			statsFileWriter.write("p,s,p&s,totalNumberOfCandidatesinML,numberOfExpiredCandidatesinML,numberOfExpiredCandidatesAfterTheMaintenanceinML,numberOfExpiredElementsInTheView,numberOfExpiredElementsInTheViewAfterTheMaintenance \n");
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	protected String getSuffix(){
		return "";
	}

	public void process(long evaluationTime, Map<Long,Integer> mentionList,Map<Long,Long> usersTimeStampOfTheCurrentSlidedWindow){			
		try {
			//skip the first iteration
			long windowDiff = evaluationTime-Config.INSTANCE.getQueryStartingTime();
			if (windowDiff==0) return;
			//			int index=((int)windowDiff)/(Config.INSTANCE.getQueryWindowWidth()*1000);			
			//			HashMap<Long,Integer> mentionList = tsc.windows.get(index);		

			
			//for statistics
			freshFollowerCount = TwitterFollowerCollector.getFollowerListFromDB(evaluationTime);
			
			//for query with filtering
			//Map<Long,Integer> FilteredMentionList = FilterUsers (mentionList);
			
			Double E = computeWindowError(mentionList,evaluationTime);
			Double E_b = computeReplicaError(evaluationTime );

			//invoke FollowerTable::getFollowers(user,ts) and updates the replica for a subset of users that exist in stream
			HashMap<Long,String> electedElements = updatePolicy(mentionList.keySet().iterator(),usersTimeStampOfTheCurrentSlidedWindow, evaluationTime);

			selectedCondidatesFileWriter.write(electedElements.toString()+","+evaluationTime+"\n");

			//update the users
			for(long id : electedElements.keySet()){
				//read the new value (= invoke the remote service)
				int newValue = TwitterFollowerCollector.getUserFollowerFromDB(evaluationTime, id);
				if(!followerReplica.get(id).equals(newValue)){
					followerReplica.put(id,newValue);
					estimatedLastChangeTime.put(id, evaluationTime);
				} else {
					//System.out.println(followerReplica.get(id) + " is equal to " + newValue);
				}
				bkgLastChangeTime.put(id, TwitterFollowerCollector.getPreviousExpTime(id,evaluationTime));
				userInfoUpdateTime.put(id, evaluationTime);
			}

			//for stats
			Double EP = computeWindowError( mentionList,evaluationTime);
			Double EP_b = computeReplicaError(evaluationTime);
			statsFileWriter.write(","+mentionList.size()+","+E+","+EP+","+E_b+","+EP_b+" \n");

			//perform the join	
			
			if (!Config.INSTANCE.getTopkQuery() ) {
				
				Iterator<Long> it= mentionList.keySet().iterator();
				while(it.hasNext()){
					long userId=Long.parseLong(it.next().toString());
					Integer userFollowers = followerReplica.get(userId);
					if (userFollowers == null) userFollowers =0;
					
					//query contains filtering part
					if ( Config.INSTANCE.getQueryWithFiltering() ){
						if (userFollowers > Config.INSTANCE.getQueryFilterThreshold()){
							answersFileWriter.write(userId +" "+mentionList.get(userId)+" "+userFollowers+" "+evaluationTime+"\n");
						}
					}
					//query without filtering part
					else{
						answersFileWriter.write(userId +" "+mentionList.get(userId)+" "+userFollowers+" "+evaluationTime+"\n");	
					}	
				}
			}
			// top-k query
			else{
				
				HashMap<Long, Float> sortedUser = new HashMap<Long, Float>();
				sortedUser = ScoringFunction.getSortedUsers(mentionList, followerReplica);
				int topk = Config.INSTANCE.getK();
				int rank = 1;
				
				Iterator<Long> sortIt= sortedUser.keySet().iterator();
				while(sortIt.hasNext() && topk > 0 ){
					
					long userId=Long.parseLong(sortIt.next().toString());
					Integer userFollowers = followerReplica.get(userId);
					if (userFollowers == null) userFollowers =0;
					answersFileWriter.write(userId +" "+mentionList.get(userId)+" "+userFollowers+" "+evaluationTime+  " " + sortedUser.get(userId)+ " " + rank +"\n");
					rank ++;
					topk--;
				}	
			}
			
			
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
			//System.out.println(userId);
			if(freshFollowerCount.get(userId)==null || freshFollowerCount.get(userId).intValue()!=followerReplica.get(userId).intValue())
			//if(TwitterFollowerCollector.getUserFollowerFromDB(evaluationTime, userId)!=followerReplica.get(userId).intValue())
				error+=1;
		}
		return error;
	}

	private double computeReplicaError(long evaluationTime){
		double error=0;
		//Iterator<Long> allUserCountIt = freshFollowerCount.keySet().iterator();
		Iterator<Long> allUserCountIt = followerReplica.keySet().iterator();
		while(allUserCountIt.hasNext()){
			Long userId=allUserCountIt.next();
			//System.out.println(userId +"   " + freshFollowerCount.get(userId));
			//System.out.println(userId +"    " +followerReplica.get(userId));
			if(freshFollowerCount.get(userId)==null || freshFollowerCount.get(userId).intValue()!=followerReplica.get(userId).intValue())
			//if(TwitterFollowerCollector.getUserFollowerFromDB(evaluationTime, userId)!=followerReplica.get(userId).intValue())
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
		int newValue = TwitterFollowerCollector.getUserFollowerFromDB(timestamp, id);
		//System.out.println(newValue);
		if(followerReplica.get(id).equals(newValue))
			return false;
		return true;
	}
	
	private Map<Long,Integer> FilterUsers (Map<Long,Integer> mentionList){

		Map<Long, Integer> mentionListTemp = new HashMap<Long, Integer>();
		// System.out.println("------------------------------------------ mention list -------------");
		// printList(mentionList);
		Iterator<Long> windowContent = mentionList.keySet().iterator();
		while (windowContent.hasNext()) {
			Long userId = windowContent.next();
			if (followerReplica.get(userId).intValue() >= Config.INSTANCE.getQueryFilterThreshold())
				mentionListTemp.put(userId, mentionList.get(userId));
		}
		// System.out.println("-------------------------------------- filtered mention list -------------");
		printList(mentionListTemp);
		return mentionListTemp;
	}
	
	private void printList (Map<Long,Integer> mentionList){
		
		Iterator<Long> windowContent = mentionList.keySet().iterator();
		while(windowContent.hasNext())	
		{
			Long userId=windowContent.next();
			//System.out.println("userId = " + userId + "followerCount = " + freshFollowerCount.get(userId).intValue())  ;
		}
	}

	protected abstract HashMap<Long,String> updatePolicy(Iterator<Long> CandidateIds,Map<Long,Long> candidateUserSetIterator, long evaluationTime );
	
		
}
