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

public class StreamGeneratorPoisson extends StreamGenerator {
	private double lambda;
	private RandomStream streamArr = new MRG32k3a(); // Events' arrivals
	private RandomStream streamIds = new MRG32k3a(); // Events' ids

	public StreamGeneratorPoisson(long initialTimestamp, int maxId, long finalTimestamp,
			boolean isForSingleID, double lambda) {
		super(initialTimestamp, maxId, finalTimestamp, isForSingleID);
		this.lambda = lambda;
	}

	protected int nextId() {
		if (this.isForSingleID)
			return this.maxId;
		return (int) Math.floor(UniformGen.nextDouble(streamIds, 0f, maxId));
	}

	protected int nextDelay() {
		return nextDelay(this.lambda);
	}

	protected int nextDelay(double lambda) {
		return PoissonGen.nextInt(streamArr, lambda);
	}

	public static void main(String[] args) {
		StreamGeneratorPoisson sg = new StreamGeneratorPoisson(0, 100, 1200L, true, 10.0);
		while (sg.hasNext())
			System.out.println(sg.next());
		// sg.simul();
		System.out.println(sg.getDescription());
	}

}
