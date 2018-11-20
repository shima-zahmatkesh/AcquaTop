package acqua.data.generator.stream.impl;

import java.util.Random;

import acqua.data.generator.stream.StreamGenerator;

public class StreamGeneratorNaive extends StreamGenerator {
	private Random rand;
	protected double lambda = 10.0;
	
	public StreamGeneratorNaive(long initialTimestamp, int maxId, int totalEvents) {
		super(initialTimestamp, maxId, totalEvents);
		rand = new Random(System.currentTimeMillis());
	}

	public StreamGeneratorNaive(long initialTimestamp, int maxId, long finalTimestamp) {
		super(initialTimestamp, maxId, finalTimestamp);
		rand = new Random(System.currentTimeMillis());
	}

	public StreamGeneratorNaive(long initialTimestamp, int maxId, long finalTimestamp,
			boolean isForSingleID, double lambda) {
		super(initialTimestamp, maxId, finalTimestamp, isForSingleID);
		this.lambda = lambda;
		rand = new Random(System.currentTimeMillis());
	}

	protected int nextDelay() {
		return nextDelay(this.lambda);
	}

	@Override
	protected int nextDelay(double lambda) {
		// System.out.println(lambda);
		return (int) Math.round(-Math.log(1.0 - rand.nextDouble()) * lambda);
	}

	protected int nextId() {
		if (isForSingleID)
			return maxId;
		return rand.nextInt(maxId);
	}

	public static void main(String[] args) {
		StreamGenerator sg = new StreamGeneratorNaive(0, 10, 100);
		while (sg.hasNext()) {
			System.out.println(sg.next());
		}
	}

	@Override
	public String getDescription() {
		return super.getDescription() + ", implParams: [lambda:" + lambda + "]";
	}

}
