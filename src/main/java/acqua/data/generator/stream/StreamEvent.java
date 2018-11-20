package acqua.data.generator.stream;

public class StreamEvent {
	long timestamp;
	int elem;

	StreamEvent(long timestamp, int elem){
		this.timestamp = timestamp;
		this.elem = elem;
	}
	
	public long getTimestamp() {
		return timestamp;
	}
	
	public long getId(){
		return elem;
	}
	
	@Override
	public String toString() {
		return timestamp+","+elem;
	}
}