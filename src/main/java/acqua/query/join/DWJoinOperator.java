package acqua.query.join;

import java.util.HashSet;
import java.util.Iterator;


public class DWJoinOperator extends ApproximateJoinOperator{
			
			
			protected HashSet<Long> updatePolicy(Iterator<Long> candidateUserSetIterator){
				return new HashSet<Long>();
			}
		}