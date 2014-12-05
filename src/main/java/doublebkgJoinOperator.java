import java.util.HashSet;
import java.util.Iterator;


public class doublebkgJoinOperator extends ApproximateJoinOperator {
	protected int updateBudget;
	doublebkgJoinOperator(int ub){
		updateBudget=ub;
	}
		protected HashSet<Long> updatePolicy(Iterator<Long> candidateUserSetIterator){
			HashSet<Long> result=new HashSet<Long>();		
			
			return result;
		}
}
