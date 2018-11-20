package acqua.data.generator.stream.impl;

import java.util.Random;

import acqua.data.generator.stream.StreamGenerator;
import umontreal.iro.lecuyer.randvar.PoissonGen;
import umontreal.iro.lecuyer.randvar.UniformGen;
import umontreal.iro.lecuyer.rng.MRG32k3a;
import umontreal.iro.lecuyer.rng.RandomStream;
import umontreal.iro.lecuyer.simprocs.ProcessSimulator;
import umontreal.iro.lecuyer.simprocs.SimProcess;
import umontreal.iro.lecuyer.simprocs.ThreadProcessSimulator;

public class StreamGeneratorPoissonNonHomo extends StreamGenerator {
	private double highMean; // this is actaull mean, not lambda. it is 1/lambda
	private double lowMean; // this is actaull mean, not lambda. it is 1/lambda
	private long highLowChangeInterval;
	private RandomStream streamLow = new MRG32k3a(); // Events' arrivals
	private RandomStream streamHigh = new MRG32k3a(); // Events' ids

	public StreamGeneratorPoissonNonHomo(long initialTimestamp, int maxId, long finalTimestamp,
			boolean isForSingleID, double highMean, double lowMean, long highLowChangeInterval) {
		super(initialTimestamp, maxId, finalTimestamp, isForSingleID);
		this.highMean = highMean;
		this.lowMean = lowMean;
		this.highLowChangeInterval = highLowChangeInterval;
	}

	protected int nextId() {
		if (this.isForSingleID)
			return this.maxId;
		else
			throw new RuntimeException();
	}

	protected int nextDelay() {
		// high->low->high->low
		if ((this.currentTimestamp / this.highLowChangeInterval) % 2 == 1)
			return nextDelay(false);
		else
			return nextDelay(true);
	}

	protected int nextDelay(boolean isHigh) {
		if (isHigh)
			return PoissonGen.nextInt(streamHigh, this.highMean);
		else
			return PoissonGen.nextInt(streamLow, this.lowMean);
	}

	public static void main(String[] args) {
		StreamGeneratorPoissonNonHomo sg = new StreamGeneratorPoissonNonHomo(0, 20, 2000L, true,
				10, 2, 100);
		while (sg.hasNext())
			System.out.println(sg.next());
	}

	@Override
	protected int nextDelay(double lambda) {
		// TODO Auto-generated method stub
		return 0;
	}

}
