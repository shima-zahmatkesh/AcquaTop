package acqua.query.join;

import java.util.Map;

public interface JoinOperator {
	public void process(long timeStamp, Map<Long,Integer> streamWindow,Map<Long,Long> usersTimeStampOfTheCurrentSlidedWindow);
	public void close();
}
