package acqua.data.generator.BKG;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

public class BKGGenerator {
	protected int maxID;
	protected Random rand;
	Map<String, Double> intervalAssignment;
	protected boolean isLinear;
	protected int minInterval;
	protected int maxInterval;

	public BKGGenerator(int maxID) {
		this.maxID = maxID;
		this.intervalAssignment = null;
		this.rand = new Random(System.currentTimeMillis());
		this.intervalAssignment = new TreeMap<String, Double>();
	}

	public BKGGenerator(int maxID, Map<String, Double> intervalAssignment) {
		this(maxID);
		this.intervalAssignment = intervalAssignment;
	}
	public BKGGenerator(int maxID, boolean isLinear, int min, int max) {
		this(maxID);
		this.isLinear = isLinear;
		this.minInterval = min;
		this.maxInterval = max;
		generateIntervalAssignment(isLinear, min, max);
	}

	public void generateIntervalAssignment(boolean isLinear, int min, int max) {
		if (isLinear) {
			double step = (double) (max - min) / (double) maxID;
			for (int i = 1; i <= this.maxID; i++) {
				this.intervalAssignment.put(String.valueOf(i), (double) min + Math.floor(step * i));
			}

		} else {
			for (int i = 1; i <= this.maxID; i++) {
				this.intervalAssignment.put(String.valueOf(i), (double) rand.nextInt(Math.abs(max
						- min))
						+ Math.min(min, max));
			}
		}

	}

	public void changeFrequencyWriter() {
		String outputBase = "./data/BKG/";
		long currentTimeStamp = System.currentTimeMillis();
		File stat = new File(outputBase+"BKG_stat_"+currentTimeStamp+".txt");
		File outputFile = new File(outputBase+"BKG_"+currentTimeStamp+".txt");
		
		BufferedWriter wr;
		try {
			wr = new BufferedWriter(new FileWriter(stat));
			wr.write("Generated: " + getClass()+"\nmaxID: "+ this.maxID+"\nisLinear: "+ this.isLinear+"\nmin: "+ this.minInterval+"\nmax: "+ this.maxInterval+"\n");
			wr.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			wr = new BufferedWriter(new FileWriter(outputFile));
			for (int i = 1; i <= maxID; i++) {
				wr.write("<http://myexample.org/S" + i + "> "
						+ (intervalAssignment.get(String.valueOf(i)).intValue()) + " "+"\n");
			}
			wr.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		for (int i = 1; i <= maxID; i++) {
			System.out.println("<http://myexample.org/S" + i + "> "
					+ (intervalAssignment.get(String.valueOf(i)).intValue()) + " ");
		}
		System.out.println("Generated: " + getClass()+"\nmaxID: "+ this.maxID+"\nisLinear: "+ this.isLinear+"\nmin: "+ this.minInterval+"\nmax: "+ this.maxInterval+"\n");
		
	}

	public static void main(String[] agrs) {
		//BKGGenerator myBKG = new BKGGenerator(Integer.parseInt(agrs[0]), Boolean.parseBoolean(agrs[1]), Integer.parseInt(agrs[2]), Integer.parseInt(agrs[3]));
		BKGGenerator myBKG = new BKGGenerator(500, false, 100, 300);
		
		myBKG.changeFrequencyWriter();
	}
}
