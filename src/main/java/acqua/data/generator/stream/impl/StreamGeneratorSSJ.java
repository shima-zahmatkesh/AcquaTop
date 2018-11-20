package acqua.data.generator.stream.impl;

import acqua.data.generator.stream.StreamGenerator;
import umontreal.iro.lecuyer.randvar.PoissonGen;
import umontreal.iro.lecuyer.randvar.UniformGen;
import umontreal.iro.lecuyer.rng.MRG32k3a;
import umontreal.iro.lecuyer.rng.RandomStream;
import umontreal.iro.lecuyer.simprocs.ProcessSimulator;
import umontreal.iro.lecuyer.simprocs.SimProcess;
import umontreal.iro.lecuyer.simprocs.ThreadProcessSimulator;

public class StreamGeneratorSSJ extends StreamGenerator {
	private SimProcess nextEvent;
	private RandomStream streamArr = new MRG32k3a(); // Events' arrivals
	private RandomStream streamIds = new MRG32k3a(); // Events' ids

	public StreamGeneratorSSJ(long initialTimestamp, int maxId, int totalEvents) {
		super(initialTimestamp, maxId, totalEvents);
	}

	public StreamGeneratorSSJ(long initialTimestamp, int maxId, int totalEvents,
			boolean isForSingleID) {
		super(initialTimestamp, maxId, totalEvents, isForSingleID);
	}

	protected int nextId() {
		if (this.isForSingleID)
			return this.maxId;
		return (int) Math.floor(UniformGen.nextDouble(streamIds, 0f, maxId));
	}

	protected int nextDelay() {
		return nextDelay(40.0);
	}

	protected int nextDelay(double lambda) {
		return PoissonGen.nextInt(streamArr, lambda);
	}

	class StreamEventImpl extends SimProcess {
		long timestamp;
		int elem;

		public StreamEventImpl(ProcessSimulator sim, long timestamp) {
			super(sim);
			this.timestamp = timestamp;
		}

		@Override
		public void actions() {
			if (timestamp < 1000) {
				int delay = PoissonGen.nextInt(streamArr, 40.0);
				nextEvent = new StreamEventImpl(sim, timestamp + delay);
				nextEvent.schedule(delay);
				elem = (int) Math.floor(UniformGen.nextDouble(streamIds, 0f, 1f) * 10);

				System.out.println(getTimestamp() + " " + getId());
			} else {
				sim.stop();
			}
		}

		public long getTimestamp() {
			return timestamp;
		}

		public long getId() {
			return elem;
		}

	}

	ProcessSimulator sim = new ThreadProcessSimulator();

	public void simul() {
		sim.init();
		SimProcess se = new StreamEventImpl(sim, 0);
		se.schedule(10.0);
		sim.start();
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		StreamGeneratorSSJ sg = new StreamGeneratorSSJ(0, 20, 100);
		while (sg.hasNext())
			System.out.println(sg.next());
		 sg.simul();
		System.out.println(sg.getDescription());
	}

}
