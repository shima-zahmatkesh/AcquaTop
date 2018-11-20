package acqua.data.generator.stream.impl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import acqua.data.generator.BKG.BKGGenerator;
import acqua.data.generator.stream.StreamGeneratorMulti;

public class StreamGeneratorMultiBasic extends StreamGeneratorMulti {
	Map<String, Double> lambdaAssignment;
	protected boolean isLinear;
	protected int minLambda;
	protected int maxLambda;
	protected Random rand;
	protected long outTimeStamp = System.currentTimeMillis();

	public StreamGeneratorMultiBasic(int maxID) {
		super(maxID);
	}

	public StreamGeneratorMultiBasic(int maxID, Map<String, Double> lambdaAssignment) {
		super(maxID);
		this.lambdaAssignment = lambdaAssignment;
		for (int i = 0; i < maxID; i++) {
			streamGen[i] = new StreamGeneratorNaive(0, i, 1200l, true, lambdaAssignment.get(String
					.valueOf(i)));
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		for (int i = 0; i < maxID; i++) {
			if (streamGen[i].hasNext())
				streamQueue.add(streamGen[i].next());
		}
	}

	public StreamGeneratorMultiBasic(long initialTimeStamp, int maxID, long endTimeStamp,
			boolean isLinear, int min, int max) {
		super(maxID);
		this.isLinear = isLinear;
		this.minLambda = min;
		this.maxLambda = max;
		this.lambdaAssignment = new TreeMap<String, Double>();
		rand = new Random(System.currentTimeMillis());
		generateLambdaAssignment(isLinear, min, max);

		for (int i = 0; i < maxID; i++) {
			streamGen[i] = new StreamGeneratorNaive(initialTimeStamp, i, endTimeStamp, true,
					lambdaAssignment.get(String.valueOf(i)));
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		for (int i = 0; i < maxID; i++) {
			if (streamGen[i].hasNext())
				streamQueue.add(streamGen[i].next());
		}

	}

	public void generateLambdaAssignment(boolean isLinear, int min, int max) {
		if (isLinear) {
			double step = (double) (max - min) / (double) maxID;
			for (int i = 1; i <= this.maxID; i++) {
				this.lambdaAssignment
						.put(String.valueOf(i - 1), (double) ((double) min + step * i));
			}

		} else {
			for (int i = 1; i <= this.maxID; i++) {
				this.lambdaAssignment.put(String.valueOf(i - 1), (double) (rand.nextDouble()
						* (Math.abs(max - min)) + Math.min(max, min)));
			}
		}

	}

	public void statWriter() {
		String outputBase = "./data/stream_multi/";

		File stat = new File(outputBase + "stream_stat_" + this.outTimeStamp + ".txt");

		BufferedWriter wr;
		try {
			wr = new BufferedWriter(new FileWriter(stat));
			wr.write(this.getDescription());

			for (int i = 1; i <= maxID; i++) {
				wr.write("<http://myexample.org/S" + i + "> "
						+ (this.lambdaAssignment.get(String.valueOf(i - 1))) + " " + "\n");
			}
			wr.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		for (int i = 1; i <= maxID; i++) {
			System.out.println("<http://myexample.org/S" + i + "> "
					+ (lambdaAssignment.get(String.valueOf(i - 1))) + " ");
		}
		System.out.println(this.getDescription());

	}

	public void streamWriter() {
		String outputBase = "./data/stream_multi/";

		File outputFile = new File(outputBase + "stream_" + this.outTimeStamp + ".txt");

		BufferedWriter wr;

		try {
			wr = new BufferedWriter(new FileWriter(outputFile));
			while (this.hasNext()) {
				String line = this.next().toString();
				wr.write(line + "\n");
			}
			wr.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		while (this.hasNext()) {
			String line = this.next().toString();
			System.out.println(line);
		}

	}

	@Override
	public String getDescription() {
		return super.getDescription() + "isLinear: " + this.isLinear + "\nmin: " + this.minLambda
				+ "\nmax: " + this.maxLambda + "\n";
	}

	public static void main(String[] agrs) {
		// StreamGeneratorMultiBasic myStream = new StreamGeneratorMultiBasic(Integer.parseInt(agrs[0]), Boolean.parseBoolean(agrs[1]), Integer.parseInt(agrs[2]), Integer.parseInt(agrs[3]));
		StreamGeneratorMultiBasic myStream = new StreamGeneratorMultiBasic(0L, 1000, 120000L, false, 200,
				100);
		// myStream.statWriter();
		myStream.streamWriter();
		myStream.statWriter();
	}
}
