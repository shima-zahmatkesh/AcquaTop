package acqua.data.generator.stream.impl;
import java.util.Random;
import java.util.function.Consumer;

import org.apache.commons.math3.distribution.PoissonDistribution;

import acqua.data.generator.stream.StreamEvent;
import acqua.data.generator.stream.StreamGenerator;

public class StreamGeneratorCommonsMath extends StreamGenerator {
	private Random rand;
	private PoissonDistribution pd;
	private double lambda = 40.0;
	
	public StreamGeneratorCommonsMath(long initialTimestamp, int maxId, int totalEvents) {
		super(initialTimestamp, maxId, totalEvents);
		rand = new Random(System.currentTimeMillis());
		pd = new PoissonDistribution(lambda);
	}
	
	public int nextDelay(){
		return pd.sample();
	}
	
	public int nextId(){
		return rand.nextInt(maxId);
	}
	
	public static void main(String[] args) {
		StreamGenerator sg = new StreamGeneratorCommonsMath(0, 10, 100);
		while(sg.hasNext()){
			System.out.println(sg.next());
		}
	}
	
	@Override
	public String getDescription() {
		return super.getDescription()+", implParams: [lambda:"+lambda+"]";
	}

	public void forEachRemaining(Consumer<? super StreamEvent> action) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected int nextDelay(double lambda) {
		throw new RuntimeException("not implemented");
	}
}
