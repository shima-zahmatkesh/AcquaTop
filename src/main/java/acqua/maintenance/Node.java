package acqua.maintenance;

public class Node {
	
	private long objectId;
	private long time;
	private float score;
	private int startWin;
	private int endWin;
	
	
	public Node(Long id, float score, int startWin, int endWin , long time) {
		super();
		this.objectId = id;
		this.score = score;
		this.startWin = startWin;
		this.endWin = endWin;
		this.time = time;
	}

	public Node(){
		
	}
	
	public Long getObjectId() {
		return objectId;
	}
	public void setObjectId(Long id) {
		this.objectId = id;
	}
	public float getScore() {
		return score;
	}
	public void setScore(float score) {
		this.score = score;
	}
	public int getStartWin() {
		return startWin;
	}
	public void setStartWin(int startWin) {
		this.startWin = startWin;
	}
	public void addStartWin() {
		this.startWin = this.startWin +1;
	}
	public int getEndWin() {
		return endWin;
	}
	public void setEndWin(int endWin) {
		this.endWin = endWin;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}
	
	public Key getKey(){
		return new Key(this.objectId ,  this.score );
	}
	
	
	
	

}
