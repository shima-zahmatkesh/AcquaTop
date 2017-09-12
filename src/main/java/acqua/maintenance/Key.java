package acqua.maintenance;

public class Key {
	
	private long objectId;
	private float score;
	
	
	
	public Key(long objectId, float score) {
		super();
		this.objectId = objectId;
		this.score = score;
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
