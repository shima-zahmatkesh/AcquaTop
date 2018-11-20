package acqua.data;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import twitter4j.JSONArray;
import twitter4j.JSONObject;

import acqua.config.Config;

public class RandomStreamParser {

	public ArrayList<ArrayList<HashMap<Long, Integer>>> windowsWithSlideEntries;
	public ArrayList<ArrayList<HashMap<Long, Long>>> slidedWindowUsersTimeStamp;

	public RandomStreamParser() {
		windowsWithSlideEntries = new ArrayList<ArrayList<HashMap<Long, Integer>>>();
		slidedWindowUsersTimeStamp = new ArrayList<ArrayList<HashMap<Long, Long>>>();
	}

	public ArrayList<ArrayList<HashMap<Long, Integer>>> extractSlides(int windowMinutesLength, int slideMinutesLength, String StreamFile) {
		long Wstart = Config.INSTANCE.getQueryStartingTime();
		long Sstart = Wstart;
		try {

			OutputStream fos;
			BufferedWriter bw;
			fos = new FileOutputStream(Config.INSTANCE.getProjectPath() + Config.INSTANCE.getDatasetFolder() + "Debug/randomFundCountSlides.txt");
			bw = new BufferedWriter(new OutputStreamWriter(fos));

			InputStream fis = new FileInputStream(StreamFile);
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
			String line = br.readLine();
			Queue<HashMap<Long, Integer>> mapOfFundCount = new LinkedList<HashMap<Long, Integer>>();
			HashMap<Long, Integer> slideMapOfFundCount = new HashMap<Long, Integer>();

			Queue<HashMap<Long, Long>> mapOfFundCountTime = new LinkedList<HashMap<Long, Long>>();
			HashMap<Long, Long> slideMapOfFundCountTime = new HashMap<Long, Long>();

			Queue<Long> slideStarts = new LinkedList<Long>();

			HashMap<Long, Long> currentWindowFundTimestamp = new HashMap<Long, Long>();

			boolean slideStartingTimeIsrecorded = false;

			while (line != null) {
				String[] timeId = line.split(",");
				long current = Long.parseLong(timeId[0]);

				if (!(current - Wstart > windowMinutesLength)) {// checking window boundaries => iterating over windows

					if (current - Sstart < slideMinutesLength) {// checking slide boundaries=>iterating over slides
						slideStartingTimeIsrecorded = false;
						Object numberOfMentions = slideMapOfFundCount.get(timeId[1]);
						if (numberOfMentions != null) {
							slideMapOfFundCount.put(Long.parseLong(timeId[1]), Integer.parseInt(numberOfMentions.toString()) + 1);
						} else {
							slideMapOfFundCount.put(Long.parseLong(timeId[1]), 1);
						}
						slideMapOfFundCountTime.put(Long.parseLong(timeId[1]), current);
					} else {
						slideStartingTimeIsrecorded = true;
						// end of current slide. adding the current slide and setting varaibles for the next slide
						bw.write(Sstart + " to " + current + " slide" + slideMapOfFundCount.toString() + "\n");
						bw.write("time : " + slideMapOfFundCountTime.toString() + "\n");
						Sstart += Config.INSTANCE.getQueryWindowSlide();// setting the start time of the next slide
						slideStarts.add(Sstart);// adding the slides start time for sliding windows
						mapOfFundCount.add((HashMap<Long, Integer>) slideMapOfFundCount.clone());// adding the copy of current slide to the current window
						mapOfFundCountTime.add((HashMap<Long, Long>) slideMapOfFundCountTime.clone());// adding the copy of current slide to the current window
						slideMapOfFundCount.clear();// clearing the current slide to be filled again from stream
						slideMapOfFundCountTime.clear();// clearing the current slide to be filled again from stream
						continue;
					}

				} else {
					if (!slideStartingTimeIsrecorded) {

						Sstart += Config.INSTANCE.getQueryWindowSlide();// setting the start time of the next slide
						slideStarts.add(Sstart);// adding the slides start time for sliding windows
						mapOfFundCount.add((HashMap<Long, Integer>) slideMapOfFundCount.clone());// adding the copy of current slide to the current window
						mapOfFundCountTime.add((HashMap<Long, Long>) slideMapOfFundCountTime.clone());// adding the copy of current slide to the current window
						slideMapOfFundCount.clear();// clearing the current slide to be filled again from stream
						slideMapOfFundCountTime.clear();// clearing the current slide to be filled again from stream
					}

					// end of current window . adding the current window and setting variables for the next window
					try {
						Wstart = slideStarts.poll();
					} catch (Exception e) {
						System.out.println(Sstart);
					}// setting the start time of the next window
					// System.out.println(mapOfUserMentions.toString());
					mapOfFundCount.add((HashMap<Long, Integer>) slideMapOfFundCount.clone());// adding the current slide to the slides of the current window
					mapOfFundCountTime.add((HashMap<Long, Long>) slideMapOfFundCountTime.clone());// adding the current slide to the slides of the current window
					bw.write(Wstart + " TO " + current + " Window : " + mapOfFundCount.toString() + "\n");
					bw.write("time : " + mapOfFundCountTime.toString() + "\n");
					windowsWithSlideEntries.add(new ArrayList<HashMap<Long, Integer>>(mapOfFundCount));// adding the current window to the list of windows with slided entries
					slidedWindowUsersTimeStamp.add(new ArrayList<HashMap<Long, Long>>(mapOfFundCountTime));// adding the list of user-entrance-timestamp for current window

					/*HashMap<Long,Integer> evictedUsers = mapOfFundCount.poll();//evict the first slide from the slides of current window
					HashMap<Long,Integer> partialLastSlideEntries = ((LinkedList<HashMap<Long,Integer>>)mapOfFundCount).removeLast();//to remove the partially added slide because it will be added fully in the next iteration
					HashMap<Long,Long> evictedUsersTime = mapOfFundCountTime.poll();//evict the first slide from the slides of current window
					HashMap<Long,Long> partialLastSlideEntriesTime = ((LinkedList<HashMap<Long,Long>>)mapOfFundCountTime).removeLast();//to remove the partially added slide because it will be added fully in the next iteration
					//evicted users should be evicted from the list of user-entrance-timestamps for the current window to be used for the next sliding window
					Iterator<Long> evictedUserIt=evictedUsers.keySet().iterator();
					while(evictedUserIt.hasNext()){
						long euid=evictedUserIt.next();
						boolean flage=false;
						for(HashMap<Long,Integer> mapOfUserMentionsSlide : mapOfFundCount ){
							if (mapOfUserMentionsSlide.get(euid)!=null)//iterate through all slides
							{
								flage=true;
								break;
							}
						}
						if(!flage) currentWindowFundTimestamp.remove(euid);
					}
					*/
					/*Iterator<Long> currentWindowUsersTimestampIt=currentWindowUsersTimestamp.keySet().iterator();
					while(currentWindowUsersTimestampIt.hasNext()){
						Long next = currentWindowUsersTimestampIt.next();
						if(partialLastSlideEntries.get(next)==null)
						currentWindowUsersTimestamp.remove(next);
					}*/

					continue;
				}
				line = br.readLine();
			}
			mapOfFundCount.add((HashMap<Long, Integer>) slideMapOfFundCount.clone());
			windowsWithSlideEntries.add(new ArrayList<HashMap<Long, Integer>>(mapOfFundCount));
			// slidedWindowUsersTimeStamp.add((HashMap<Long,Long>)currentWindowUsersTimestamp.clone());
			mapOfFundCountTime.add((HashMap<Long, Long>) slideMapOfFundCountTime.clone());
			slidedWindowUsersTimeStamp.add(new ArrayList<HashMap<Long, Long>>(mapOfFundCountTime));
			// slidedWindowUsersTimeStamp.add((HashMap<Long,Long>)currentWindowUsersTimestamp.clone());
			br.close();
			bw.flush();
			bw.close();
			return windowsWithSlideEntries;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

	}

	public ArrayList<HashMap<Long, Integer>> aggregateSildedWindowsFund() {
		ArrayList<HashMap<Long, Integer>> slidedWindows = new ArrayList<HashMap<Long, Integer>>();
		for (int i = 0; i < windowsWithSlideEntries.size(); i++) {
			ArrayList<HashMap<Long, Integer>> tempSplittedWindow = windowsWithSlideEntries.get(i);
			HashMap<Long, Integer> WindowUserMention = new HashMap<Long, Integer>();
			for (int j = 0; j < tempSplittedWindow.size(); j++) {// per slide
				HashMap<Long, Integer> slideUsers = tempSplittedWindow.get(j);
				Iterator<Long> slideuserit = slideUsers.keySet().iterator();
				while (slideuserit.hasNext()) {
					long slideUserName = slideuserit.next();
					Integer windowusercount = WindowUserMention.get(slideUserName);
					if (windowusercount == null) {
						WindowUserMention.put(slideUserName, slideUsers.get(slideUserName));
					} else {
						WindowUserMention.put(slideUserName, slideUsers.get(slideUserName) + windowusercount);
					}
				}
			}
			slidedWindows.add(WindowUserMention);
		}

		return slidedWindows;
	}

	public ArrayList<HashMap<Long, Long>> aggregateSildedWindowsFundTime() {
		ArrayList<HashMap<Long, Long>> slidedWindowsTime = new ArrayList<HashMap<Long, Long>>();
		for (int i = 0; i < slidedWindowUsersTimeStamp.size(); i++) {
			ArrayList<HashMap<Long, Long>> tempSplittedWindowTime = slidedWindowUsersTimeStamp.get(i);
			HashMap<Long, Long> WindowUserMentionTime = new HashMap<Long, Long>();
			for (int j = 0; j < tempSplittedWindowTime.size(); j++) {// per slide
				HashMap<Long, Long> slideUsersTime = tempSplittedWindowTime.get(j);
				Iterator<Long> slideuserit = slideUsersTime.keySet().iterator();
				while (slideuserit.hasNext()) {
					long slideUserName = slideuserit.next();
					Long windowusertime = WindowUserMentionTime.get(slideUserName);
					WindowUserMentionTime.put(slideUserName, slideUsersTime.get(slideUserName));
				}
			}
			slidedWindowsTime.add(WindowUserMentionTime);
		}
		return slidedWindowsTime;
	}

	public static void main(String[] args) {
		RandomStreamParser rsp = new RandomStreamParser();
		rsp.extractSlides(Config.INSTANCE.getQueryWindowWidth(), Config.INSTANCE.getQueryWindowSlide(), Config.INSTANCE.getProjectPath() + "stream-1428595804910.csv");
		System.out.println(rsp.aggregateSildedWindowsFund());
		System.out.println(rsp.aggregateSildedWindowsFundTime());
	}
}
