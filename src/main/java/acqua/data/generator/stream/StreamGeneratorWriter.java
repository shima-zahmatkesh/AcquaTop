package acqua.data.generator.stream;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import acqua.data.generator.stream.impl.StreamGeneratorNaiveSin;

public class StreamGeneratorWriter {
	public static void main(String[] args) throws IOException {
		String ts = Long.toString(System.currentTimeMillis());
		FileWriter fw = new FileWriter("./data/stream/stream-"+ts+".csv");
		BufferedWriter out = new BufferedWriter(fw);
		//StreamGenerator sg = new StreamGeneratorSSJ(0, 10, 300);
		
		StreamGenerator sg = new StreamGeneratorNaiveSin(0, 100, 12000l);
		
		
		while(sg.hasNext()){
			String line = sg.next().toString(); 
			out.write(line);
			out.newLine();
		}
		out.close();
		fw.close();
		fw = new FileWriter("stream-"+ts+".txt");
		fw.write(sg.getDescription());
		fw.close();
	}
}
