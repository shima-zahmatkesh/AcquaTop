package acqua.query.join.acqua;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;


public class WSTJoinOperator extends ApproximateJoinOperator{
			
			
			protected HashMap<Long,String> updatePolicy(Iterator<Long> candidateUserSetIterator,Map<Long,Long> usersTimeStampOfTheCurrentSlidedWindow, long evaluationTime){
				return new HashMap<Long,String>();
			}
		}