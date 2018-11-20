package acqua.data.generator.stream.impl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

public class StreamGeneratorMultiNonHomoPoissonParas {
	int maxID;
	Map<Integer, Double> highMap;
	Map<Integer, Double> lowMap;
	Map<Integer, Double> highLowChangeIntervalMap;
	// +-0.1 of high low
	double high;
	double low;
	double randomnessHighLow = 0.4;
	// +-0.4 of highLowChangeIntervalMap
	double changeInterval;
	double randomnessHighLowChnageInterval = 0.6;
	protected Random rand;

	public StreamGeneratorMultiNonHomoPoissonParas(int maxID, double high, double low,
			int highLowChangeInterval) {
		rand = new Random(System.currentTimeMillis());
		highMap = new TreeMap<Integer, Double>();
		lowMap = new TreeMap<Integer, Double>();
		highLowChangeIntervalMap = new TreeMap<Integer, Double>();
		this.maxID = maxID;
		this.high = high;
		this.low = low;
		this.changeInterval = highLowChangeInterval;
		for (int i = 0; i < maxID; i++) {
			double tempHigh = high * (1 - randomnessHighLow);
			tempHigh += high * randomnessHighLow * 2 * (rand.nextDouble());
			double tempLow = low * (1 - randomnessHighLow);
			tempLow += low * randomnessHighLow * 2 * (rand.nextDouble());
			double temphighLowChangeInterval = highLowChangeInterval
					* (1 - randomnessHighLowChnageInterval);
			temphighLowChangeInterval += highLowChangeInterval * randomnessHighLowChnageInterval
					* 2 * (rand.nextDouble());
			highMap.put(i, tempHigh);
			lowMap.put(i, tempLow);
			highLowChangeIntervalMap.put(i, temphighLowChangeInterval);
		}

	}

	public void printOutParas() {
		try {
			long currnetTime = System.currentTimeMillis();
			BufferedWriter wr = new BufferedWriter(new FileWriter(new File(
					"./data/stream_multi_nonHomo/paras_" + currnetTime + ".txt")));
			for (int i = 0; i < maxID; i++) {
				wr.write(i + "\t" + String.format("%.2f", highMap.get(i)) + "\t"
						+ String.format("%.2f", lowMap.get(i)) + "\t"
						+ String.format("%.2f", highLowChangeIntervalMap.get(i)) + "\n");
			}
			wr.close();
			wr = new BufferedWriter(new FileWriter(new File("./data/stream_multi_nonHomo/stat_"
					+ currnetTime + ".txt")));

			String output = "maxID: " + this.maxID + "\nhigh: " + this.high + "\nlow: " + this.low
					+ "\nrandomeness: " + this.randomnessHighLow + "\nchangeinterval: "
					+ this.changeInterval + "\nrandomess: " + this.randomnessHighLowChnageInterval
					+ "\n";
			wr.write(output);

			wr.close();
			System.out.println(output);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for (int i = 0; i < maxID; i++) {
			System.out.println(i + "\t" + String.format("%.2f", highMap.get(i)) + "\t"
					+ String.format("%.2f", lowMap.get(i)) + "\t"
					+ String.format("%.2f", highLowChangeIntervalMap.get(i)));
		}
	}

	public static void main(String[] agrs) {
		StreamGeneratorMultiNonHomoPoissonParas myParas = new StreamGeneratorMultiNonHomoPoissonParas(
				50, 100, 20, 300);
		myParas.printOutParas();
	}
}
