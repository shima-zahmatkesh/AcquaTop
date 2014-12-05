package acqua.query.join;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;


public class RandomCacheUpdateJoin extends ApproximateJoinOperator {
	protected int updateBudget;
public RandomCacheUpdateJoin(int ub) {
	updateBudget=ub;
	// TODO Auto-generated constructor stub
}
protected HashSet<Long> updatePolicy(Iterator<Long> candidateUserSetIterator){
	//decide which rows to update and return the list
	//it must satisfy the updateBudget constraint!
	//A is the list of all userids avaiable in stream
	ArrayList<Long> A=new ArrayList<Long>();
	while(candidateUserSetIterator.hasNext()){
		A.add(candidateUserSetIterator.next());
	}
	Random rand = new Random(System.currentTimeMillis());
	ArrayList<Integer> indexes = new ArrayList<Integer>();
	while(indexes.size()<updateBudget){
	indexes.add(rand.nextInt(A.size()));
	}
		HashSet<Long> result=new HashSet<Long>();
		int counter=0;
		while(counter<updateBudget){
			System.out.println("user Id: "+A.get(indexes.get(counter)));
			result.add(A.get(indexes.get(counter)));
			counter++;
		}
		System.out.println("-----------------------------------------------------------------");
		return result;
	}
}
