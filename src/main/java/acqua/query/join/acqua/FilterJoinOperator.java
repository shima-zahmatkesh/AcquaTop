package acqua.query.join.acqua;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import acqua.config.Config;
import acqua.data.TwitterFollowerCollector;

public class FilterJoinOperator extends ApproximateJoinOperator {

	protected int updateBudget;
	
	public FilterJoinOperator(int ub){
		updateBudget=ub;
	}
	
	
	@Override
	public void process(long evaluationTime, Map<Long,Integer> mentionList,Map<Long,Long> usersTimeStampOfTheCurrentSlidedWindow){	
		
		try {
			//skip the first iteration
			long windowDiff = evaluationTime-Config.INSTANCE.getQueryStartingTime();
			if (windowDiff==0) return;
			

			//for statistics
			freshFollowerCount = TwitterFollowerCollector.getFollowerListFromDB(evaluationTime);
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
			Iterator<Long> it= mentionList.keySet().iterator();
			//System.out.println("join considering filtering");
			while(it.hasNext()){
				long userId=Long.parseLong(it.next().toString());
				Integer userFollowers = followerReplica.get(userId);
				if (userFollowers > Config.INSTANCE.getQueryFilterThreshold()){
					answersFileWriter.write(userId +" "+mentionList.get(userId)+" "+userFollowers+" "+evaluationTime+"\n");
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
		while(windowContent.hasNext()){
			Long userId=windowContent.next();
			if(freshFollowerCount.get(userId).intValue()!=followerReplica.get(userId).intValue())
				error+=1;
		}
		return error;
	}

	private double computeReplicaError(long evaluationTime){
		double error=0;
		Iterator<Long> allUserCountIt = followerReplica.keySet().iterator();
		while(allUserCountIt.hasNext()){
			Long userId=allUserCountIt.next();
			if(freshFollowerCount.get(userId).intValue()!=followerReplica.get(userId).intValue())
				error+=1;
		}
		return error;
	}
	
	@Override
	protected HashMap<Long, String> updatePolicy(Iterator<Long> CandidateIds, Map<Long, Long> candidateUserSetIterator, long evaluationTime) {
		
		
		final class User{
			long userId;
			long filterDiff;
			public User(long id,long f){userId=id;filterDiff=f;}
		}
		
		List<User> userFilterDiff=new ArrayList<User>();
		while(CandidateIds.hasNext()){
			long userid= CandidateIds.next();
			long followerCount = followerReplica.get(userid);
			//System.out.println ("follower count  = " + followerCount);
			if ( Math.abs( followerCount - Config.INSTANCE.getQueryFilterThreshold() ) < Config.INSTANCE.getQueryDifferenceThreshold()){
				userFilterDiff.add(new User(userid,  Math.abs( followerCount - Config.INSTANCE.getQueryFilterThreshold() ) ));				
			}
		}
		//Ascending Order
		Collections.sort(userFilterDiff, new Comparator<User>() {

			public int compare(User o1, User o2) {
				
				int res=(int) (o2.filterDiff - o1.filterDiff);
				//System.out.println("res = " + res);
				return -res;    
			}
		});


		//////////////////////////////////////////
		
		HashMap<Long,String> result=new HashMap<Long,String>();
		Iterator<User> it = userFilterDiff.iterator();
		int counter=0;
		while(it.hasNext()&&counter<updateBudget){
			User temp = it.next();
			//System.out.println ("user id" + temp.userId + "dif" + temp.filterDiff);
			double replicaValue=followerReplica.get(temp.userId);
			double bkgValue=TwitterFollowerCollector.getUserFollowerFromDB(evaluationTime, temp.userId);
			if(replicaValue==bkgValue)
				{
				result.put(temp.userId,"=");
				}
			else 
				{
				result.put(temp.userId, "<>");
				}
			//System.out.println("id "+temp.userId+">> oldness "+ temp.filterDiff +"    chachedValue >> "+ replicaValue + "    actualValue >> "+ bkgValue +"    differenece >>" + ( bkgValue-replicaValue) + " \n");
			counter++;
		}
		//System.out.println("skipped users: ");
		while(it.hasNext()){
			User temp = it.next();
			double replicaValue=followerReplica.get(temp.userId);
			double bkgValue=TwitterFollowerCollector.getUserFollowerFromDB(evaluationTime, temp.userId);
			//System.out.printf("id "+temp.userId+">> oldness "+temp.filterDiff+"    chachedValue >> "+ replicaValue + "    actualValue >> "+ bkgValue +"    differenece >>" + ( bkgValue-replicaValue) + " \n");
		}
		return result;

	
	}

}
