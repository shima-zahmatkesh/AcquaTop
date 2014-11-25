import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;


public class randomCacheUpdateJoin extends ApproximateJoinOperator {
	protected int updateBudget;
public randomCacheUpdateJoin(int ub) {
	updateBudget=ub;
	// TODO Auto-generated constructor stub
}
protected HashSet<Long> updatePolicy(Iterator<Long> candidateUserSetIterator){
	//decide which rows to update and return the list
	//it must satisfy the updateBudget constraint!
	ArrayList<Long> A=new ArrayList<Long>();
	while(candidateUserSetIterator.hasNext()){
		A.add(candidateUserSetIterator.next());
	}
	Random rand = new Random();
	Set<Integer> indexes = new HashSet<Integer>();
	while(indexes.size()<updateBudget){
	indexes.add(rand.nextInt(A.size()));
	}
		HashSet<Long> result=new HashSet<Long>();
		int counter=0;
		while(counter<updateBudget){
			result.add(A.get(counter));
			counter++;
		}
		return result;
	}
}
