package acqua.data;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import acqua.query.window.Entry;
import acqua.query.window.SlidingWindow;
import acqua.query.window.Window;

public class SlidingWindowFromStreamGeneratorFile extends SlidingWindow {
	private Logger logger = LoggerFactory.getLogger(SlidingWindowFromStreamGeneratorFile.class);

	private InputStream fis;
	private BufferedReader br;
	private Entry currentEntry;

	public SlidingWindowFromStreamGeneratorFile(int width, int slide, long initialTimestamp, String streamFile) {
		super(width, slide, initialTimestamp);
		try {
			//System.out.println(streamFile);
			fis = new FileInputStream(streamFile);
			br = new BufferedReader(new InputStreamReader(fis));
			// read the first line
			String line = br.readLine();
			System.out.println(line);
			currentEntry = parseCurrentEntry(line);
		} catch (FileNotFoundException e) {
			logger.error("Error while reading the file", e);
		} catch (IOException e) {
			logger.error("Error while reading the line", e);
		}
	}

	protected void fillWindow(long endingTimestamp) {
		try {
			while (currentEntry != null && currentEntry.getTimestamp() <= endingTimestamp) {
				currentWindow.addEntry(currentEntry);
				currentEntry = parseCurrentEntry(br.readLine());
			}
		} catch (IOException e) {
			logger.error("Error while reading the line", e);
		}
	}

	protected Entry parseCurrentEntry(String line) {
		if (line == null)
			return null;
		String[] data = line.split(",");
		long current = Long.parseLong(data[0]);
		long id = Long.parseLong(data[1]);
		return new Entry(current, id);
	}

	public boolean hasNext() {
		try {
			if (currentEntry == null) {
				br.close();
				fis.close();
			}
		} catch (IOException e) {
			logger.error("Error while closing the file", e);
		}
		return currentEntry != null;
	}

	@Override
	public Window next() {
		super.next();
		fillWindow(currentTimestamp);
		return currentWindow;
	}

	public static void main(String[] args) {
		/*
		SlidingWindow sw = new SlidingWindowFromStreamGeneratorFile(1000, 500, 0, "stream-1428595804910.csv");
		while(sw.hasNext()){
			Window w = sw.next();
			System.out.println(w.toString());
			StringBuilder sb = new StringBuilder();
			for(Entry e : w.getDistinctEntries())
				sb.append(e.toString()+" ");
			System.out.println(sb.toString());
			sb = new StringBuilder();
			for(Entry e : w.getFrequencyOfEntities())
				sb.append(e.toString()+" ");
			System.out.println(sb.toString());
			sb = new StringBuilder();
			for(java.util.Map.Entry<Long, Long> e : w.getDistinctEntriesAsMap().entrySet())
				sb.append(e.getKey()+","+e.getValue()+" ");
			System.out.println(sb.toString());
			sb = new StringBuilder();
			for(java.util.Map.Entry<Long, Integer> e : w.getFrequencyOfEntitiesAsMap().entrySet())
				sb.append(e.getKey()+","+e.getValue()+" ");
			System.out.println(sb.toString());
		}
		*/
	}

}
