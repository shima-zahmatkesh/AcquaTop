package acqua.query.join.MTKN;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import acqua.query.join.MTKN.ApproximateJoinMTKNOperator;


public class WSTJoinOperator extends ApproximateJoinMTKNOperator{
		
		
	public WSTJoinOperator() {
		super();
	}

	protected HashMap<Long,String> updatePolicy(Iterator<Long> candidateUserSetIterator,Map<Long,Long> usersTimeStampOfTheCurrentSlidedWindow, long evaluationTime){
		return new HashMap<Long,String>();
	}
	
	
}
