package acqua.query.window;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Window {
	int id;
	private long startingTime, endingTime ;
	private List<Entry> entries;
	
	public Window(long start, long end ,int id){
		
		this.id = id;
		startingTime = start;
		endingTime =  end;
		System.out.println(" Window "+ id + " is created from time = "+ start + " to time = " + end);
		entries = new ArrayList<Entry>();
	}
	
	public Window(long start, long end, Window previousWindow , int id ){
		this(start, end, id);
		for(Entry entry : previousWindow.getEntries()){
			if(entry.getTimestamp()>=startingTime){
				entries.add(entry);
			}
		}
	}
	
	public long getEndingTime(){
		return endingTime;
	}
	public int getWindowId(){
		return (int) id;
	}
	
	public void addEntry(Entry e) {
		entries.add(e);
	}
	
	public List<Entry> getEntries(){
		return entries;
	}
	
	public List<Entry> getDistinctEntries(){
		List<Entry> ret = new ArrayList<Entry>();
		Entry[] ens = new Entry[ret.size()];
		Set<Long> seen = new HashSet<Long>();
		ens = entries.toArray(ens);
		for(int i = ens.length-1; i>=0; i--){
			Entry e = ens[i];
			if(!seen.contains(e.getId())){
				seen.add(e.getId());
				ret.add(e);
			}
		}
		return ret;
	}
	
	public List<EntryFrequency> getFrequencyOfEntities(){
		List<EntryFrequency> ret = new ArrayList<EntryFrequency>();
		Entry[] ens = new Entry[ret.size()];
		ens = entries.toArray(ens);
		for(int i = ens.length-1; i>=0; i--){
			Entry e = ens[i];
			boolean updated = false;
			for(EntryFrequency ef : ret){
				if(ef.getId()==e.getId()){
					ef.incFrequency();
					updated = true;
				}
			}
			if(!updated){
				ret.add(new EntryFrequency(e));
			} 
			
		}
		return ret;
	}

	public Map<Long,Long> getDistinctEntriesAsMap(){
		Map<Long,Long> ret = new HashMap<Long,Long>();
		for(Entry e : entries){
			ret.put(e.getId(),e.getTimestamp());
		}
		return ret;
	}
	
	public Map<Long,Integer> getFrequencyOfEntitiesAsMap(){
		Map<Long,Integer> ret = new HashMap<Long,Integer>();
		for(Entry e : entries){
			Integer t = ret.get(e.getId());
			if(t==null)
				ret.put(e.getId(),1);
			else
				ret.put(e.getId(),t+1);
		}
		return ret;
	}

	@Override
	public String toString() {
		StringBuilder ret = new StringBuilder();
		ret.append("Window ("+startingTime+","+endingTime+"]: { ");
		for(Entry e : entries)
			ret.append(e.toString()+" ");
		ret.append("}");
		return ret.toString();
	}
}
