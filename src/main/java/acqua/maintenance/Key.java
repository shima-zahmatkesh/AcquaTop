package acqua.maintenance;

public class Key {
	
	private long objectId;
	private float score;
	private long time ;
	
	
	
	public long getTime() {
		return time;
	}
	public void setTime(long time) {
		this.time = time;
	}
	public Key(long objectId, float score , long time) {
		super();
		this.objectId = objectId;
		this.score = score;
		this.time = time;
	}
	public long getObjectId() {
		return objectId;
	}
	public void setObjectId(long objectId) {
		this.objectId = objectId;
	}
	public float getScore() {
		return score;
	}
	public void setScore(float score) {
		this.score = score;
	}
	
	

}
