package acqua.data.generator.stream;

import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

public abstract class StreamGeneratorMulti implements Iterator<StreamEvent> {
	public StreamGenerator[] streamGen;
	public PriorityQueue<StreamEvent> streamQueue;
	public int maxID;

	public StreamGeneratorMulti(int maxID) {
		this.maxID = maxID;
		streamGen = new StreamGenerator[maxID];
		TimeStampSort TSs = new TimeStampSort();
		streamQueue = new PriorityQueue<StreamEvent>(maxID, TSs);
	}

	public StreamEvent next() {
		StreamEvent result = streamQueue.poll();
		int id = (int) result.getId();
		if (streamGen[id].hasNext())
			streamQueue.add(streamGen[id].next());
		return result;
	}

	public boolean hasNext() {
		if (streamQueue.size() > 0)
			return true;
		return false;
	}

	public void remove() {
		throw new RuntimeException();
	}

	public String getDescription() {
		return "Generated: " + getClass() + "\nmaxID: " + streamGen.length + "\n";
	}

	class TimeStampSort implements Comparator<StreamEvent> {
		public int compare(StreamEvent event1, StreamEvent event2) {
			return (int) (event1.getTimestamp() - event2.getTimestamp());
		}
	}
}
