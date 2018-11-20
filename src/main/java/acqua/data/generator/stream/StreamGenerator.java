package acqua.data.generator.stream;

import java.util.Iterator;

public abstract class StreamGenerator implements Iterator<StreamEvent> {
	protected long initialTimestamp, currentTimestamp, finalTimestamp;
	protected int maxId;
	protected int initialEvents, remainingEvents;
	protected boolean isForSingleID;

	public StreamGenerator(long initialTimestamp, int maxId, int totalEvents) {
		this.initialTimestamp = initialTimestamp;
		finalTimestamp = -1;
		currentTimestamp = initialTimestamp;
		initialEvents = remainingEvents = totalEvents;
		this.maxId = maxId;
	}

	public StreamGenerator(long initialTimestamp, int maxId, long finalTimestamp) {
		this.initialTimestamp = initialTimestamp;
		this.finalTimestamp = finalTimestamp;
		currentTimestamp = initialTimestamp;
		initialEvents = remainingEvents = -1;
		this.maxId = maxId;
	}

	public StreamGenerator(long initialTimestamp, int maxId, long finalTimestamp,
			boolean isForSingleID) {
		this.initialTimestamp = initialTimestamp;
		this.finalTimestamp = finalTimestamp;
		currentTimestamp = initialTimestamp;
		initialEvents = remainingEvents = -1;
		this.maxId = maxId;
		this.isForSingleID = isForSingleID;
	}

	public StreamEvent next() {
		if (initialEvents > 0)
			remainingEvents--;
		StreamEvent se = new StreamEvent(currentTimestamp, nextId());
		currentTimestamp += nextDelay();
		return se;
	};

	protected abstract int nextId();

	protected abstract int nextDelay();

	protected abstract int nextDelay(double lambda);

	public boolean hasNext() {
		if (initialEvents > 0)
			return remainingEvents > 0;
		return currentTimestamp < finalTimestamp;
	}

	public void remove() {
		throw new RuntimeException();
	}

	public String getDescription() {
		return "Generated with " + getClass() + ", commonParams: [finalTimestamp:" + finalTimestamp
				+ "; initialTimestamp:" + initialTimestamp + "; ids:[0," + maxId + "); events:"
				+ initialEvents + "]";
	}
}
