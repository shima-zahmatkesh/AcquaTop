import java.util.HashMap;


public interface JoinOperator {
	
	public void process(long timeStamp,HashMap<Long,Integer> streamWindow);
	public void close();
}
