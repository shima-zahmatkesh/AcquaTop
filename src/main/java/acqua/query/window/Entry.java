package acqua.query.window;

public class Entry {
	protected long id;
	protected long timestamp;
	
	public Entry(long timestamp, long id){
		this.id=id;
		this.timestamp=timestamp;
	}
	
	public long getId(){
		return id;
	}
	
	public long getTimestamp(){
		return timestamp;
	}
	
	@Override
	public String toString() {
		return id + "["+timestamp+"]";
	}
}
