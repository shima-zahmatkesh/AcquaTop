package acqua.query.window;

public class EntryFrequency extends Entry{
	protected int freq;
	public EntryFrequency(Entry e){
		super(e.timestamp, e.id);
		freq = 1;
	}
	
	public void incFrequency(){
		freq++;
	}
	
	public int getFrequency(){
		return freq;
	}
	
	@Override
	public String toString() {
		return id+","+freq+"["+timestamp+"]";
	}
}
