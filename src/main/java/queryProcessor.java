
public class queryProcessor {
	JoinOperator join;
	public queryProcessor(){
		
	}
	public void evaluateQuery(long timeStamp){
		join.process(timeStamp);
	}
	public interface JoinOperator{
		public void process(long timeStamp);
	}
	//----------------------------------------------------------------------------------------
	public class oracleJoinOperator implements JoinOperator{
		public void process(long timeStamp){}
	}
	//----------------------------------------------------------------------------------------
	public abstract class ApproximateJoinOperator implements JoinOperator{
		protected Map<long,int> replica;
		protected int updateBudget;

		public void process(long timestamp){
			for(long id : updatePolicy()){
				//invoke FollowerTable::getFollowers(user,ts) and updates the replica
			}
			//process the join
		}
		protected abstract Set<long> updatePolicy();
	}
	
	public class BaselineJoinOperator extends ApproximateJoinOperator{
		protected Set<long> updatePolicy(){
		//decide which rows to update and return the list
		//it must satisfy the updateBudget constraint!
		}
	}
	public static void main(String[] args){
	
	}
}
