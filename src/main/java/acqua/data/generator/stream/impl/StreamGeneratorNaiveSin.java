package acqua.data.generator.stream.impl;


public class StreamGeneratorNaiveSin extends StreamGeneratorNaive{
	double clock = 0;
	
	public StreamGeneratorNaiveSin(long initialTimestamp, int maxId, int totalEvents) {
		super(initialTimestamp, maxId, totalEvents);
	}
	
	public StreamGeneratorNaiveSin(long initialTimestamp, int maxId, long finalTimestamp) {
		super(initialTimestamp, maxId, finalTimestamp);
	}
	
	@Override
	protected int nextDelay() {
		int ret = super.nextDelay();
		lambda = Math.max(0.1, 10.0 + 18 * Math.sin(clock));
		clock += 0.5;
		return ret;
	}
	
	@Override
	public String getDescription() {
		return super.getDescription()+", implParams: [lambda: 40.0 + 20 * sin(clock); clock: clock+1]";
	}
}
