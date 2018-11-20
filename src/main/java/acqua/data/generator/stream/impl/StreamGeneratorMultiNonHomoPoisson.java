package acqua.data.generator.stream.impl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import acqua.data.generator.stream.StreamGeneratorMulti;

public class StreamGeneratorMultiNonHomoPoisson extends StreamGeneratorMultiBasic {
	Map<String, Double> lambdaAssignment;
	String inputPara;

	public StreamGeneratorMultiNonHomoPoisson(long initTimeStamp, int maxID, long endTimeStamp,
			String highLowInput) {
		super(maxID);
		this.inputPara = highLowInput;
		Map<Integer, Double> tempHigh = new TreeMap<Integer, Double>();
		Map<Integer, Double> tempLow = new TreeMap<Integer, Double>();
		Map<Integer, Double> tempChangeInterval = new TreeMap<Integer, Double>();

		try {
			BufferedReader rd = new BufferedReader(new FileReader(new File(
					"./data/stream_multi_nonHomo/" + highLowInput)));
			String input;
			while ((input = rd.readLine()) != null) {
				String[] split = input.split("\t");
				tempHigh.put(Integer.parseInt(split[0]), Double.parseDouble(split[1]));
				tempLow.put(Integer.parseInt(split[0]), Double.parseDouble(split[2]));
				tempChangeInterval.put(Integer.parseInt(split[0]), Double.parseDouble(split[3]));
			}
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		for (int i = 0; i < maxID; i++) {
			streamGen[i] = new StreamGeneratorPoissonNonHomo(initTimeStamp, i, endTimeStamp, true,
					tempHigh.get(i), tempHigh.get(i), (long) tempChangeInterval.get(i).intValue());
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

	public void statWriter() {
		String outputBase = "./data/stream_multi/";
		File stat = new File(outputBase + "stream_stat_" + this.outTimeStamp + ".txt");

		BufferedWriter wr;
		try {
			wr = new BufferedWriter(new FileWriter(stat));
			wr.write(this.getDescription() + "inputpara: " + this.inputPara);

			wr.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(this.getDescription() + "inputpara: " + this.inputPara);

	}

	public static void main(String[] agrs) {
		// StreamGeneratorMultiBasic myStream = new StreamGeneratorMultiBasic(Integer.parseInt(agrs[0]), Boolean.parseBoolean(agrs[1]), Integer.parseInt(agrs[2]), Integer.parseInt(agrs[3]));
		StreamGeneratorMultiNonHomoPoisson myStream = new StreamGeneratorMultiNonHomoPoisson(0L,
				50, 1200L, "paras_1438287347928.txt");
		// myStream.statWriter();
		myStream.streamWriter();
		myStream.statWriter();
	}
}
