package acqua.data;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import acqua.config.Config;

import twitter4j.FilterQuery;
import twitter4j.JSONArray;
import twitter4j.JSONObject;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterObjectFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.UserMentionEntity;
import twitter4j.conf.ConfigurationBuilder;


public class TwitterStreamCollector {
	private ConfigurationBuilder cb;
	private HashMap<Long,Integer> userMentionCount;
	public ArrayList<HashMap<Long, Integer>> windows;
	public ArrayList<ArrayList<HashMap<Long,Integer>>> windowsWithSlideEntries;
	public ArrayList<ArrayList<HashMap<Long,Long>>> slidedWindowUsersTimeStamp;
	public static long[] monitoredIds;
	public static String[] monitorNames;
	//constructor for not listening
	/*public TwitterStreamCollector(){
		windows=new ArrayList<HashMap<Long,Integer>>();
		extractUserIds("D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/followers.init");
	}*/

	//constructor for listening
	public TwitterStreamCollector(){
		userMentionCount=new HashMap<Long, Integer>();
		cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		.setOAuthConsumerKey(Config.INSTANCE.getTwitterConsumerKey())
		.setOAuthConsumerSecret(Config.INSTANCE.getTwitterConsumerSecret())
		.setOAuthAccessToken(Config.INSTANCE.getTwitterAccessToken())
		.setOAuthAccessTokenSecret(Config.INSTANCE.getTwitterAccessTokenSecret());
		cb.setJSONStoreEnabled(true);
		windows= new ArrayList<HashMap<Long,Integer>>();
		windowsWithSlideEntries=new ArrayList<ArrayList<HashMap<Long,Integer>>>();
		slidedWindowUsersTimeStamp=new ArrayList<ArrayList<HashMap<Long,Long>>>();
		extractUserIds(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"followers.init");	
	}
	public static void extractUserIds(String userListToMonitor){
		try{
			InputStream fis;
			BufferedReader br;
			String line;

			fis = new FileInputStream(userListToMonitor);//"D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/followers.csv");
			br = new BufferedReader(new InputStreamReader(fis));


			String br1 = br.readLine();
			String[] idStr = br1.split(",");
			monitoredIds=new long[idStr.length];

			for(int o=0;o<idStr.length;o++)
			{
				monitoredIds[o]=Long.parseLong(idStr[o]);
			}
			String br2=br.readLine();
			monitorNames=br2.split(",");
			br.close();
			br = null;
			fis = null;}catch(Exception e){e.printStackTrace();}

	}
	public void listen(String outputStreamFile){
		try{
			final FileWriter fw=new FileWriter(new File(outputStreamFile));//"D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/twitterStream.txt"));
			TwitterStream tStream = new TwitterStreamFactory(cb.build()).getInstance();
			// sample() method internally creates a thread which manipulates TwitterStream and calls these adequate listener methods continuously.
			StatusListener listener = new StatusListener() {

				public void onException(Exception ex) {

					ex.printStackTrace();
				}

				public void onTrackLimitationNotice(int arg0) {
					// TODO Auto-generated method stub

				}

				public void onStatus(Status status) {

					for(UserMentionEntity ume : status.getUserMentionEntities()){
						for(long mid : monitoredIds)
							if(mid==ume.getId()){
								String rawJSON = TwitterObjectFactory.getRawJSON(status);
								try {
									//System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
									fw.write(rawJSON+"\n");
									fw.flush();
								} catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}
					}	            				
				}

				public void onStallWarning(StallWarning stallWarning) {
					System.out.println(stallWarning);				
				}

				public void onScrubGeo(long arg0, long arg1) {
					// TODO Auto-generated method stub				
				}

				public void onDeletionNotice(StatusDeletionNotice arg0) {
					// TODO Auto-generated method stub
				}
			};	
			tStream.addListener(listener);
			FilterQuery fq=new FilterQuery();
			fq.track(monitorNames);
			tStream.filter(fq);
			//fw.flush();

		}catch(IOException e){
			e.printStackTrace();
		}
	}
	public ArrayList<HashMap<Long, Integer>> extractWindow(int windowMinutesLength, String streamFile){		
		try{
			OutputStream    fos;
			BufferedWriter bw;
			fos=new FileOutputStream(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"Debug/twitterMentionWindows.txt");
			bw=new BufferedWriter(new OutputStreamWriter(fos));
			InputStream    fis;
			BufferedReader br;

			//FIXME: remove the dependency to QueryProcessor
			long start=Config.INSTANCE.getQueryStartingTime();

			fis = new FileInputStream(streamFile);//"D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/followerSnapshotsFile2.txt");
			br = new BufferedReader(new InputStreamReader(fis));
			String line = br.readLine();

			//ArrayList<HashMap<String,Integer>> windows=new ArrayList<HashMap<String,Integer>>();
			HashMap<Long ,Integer> mapOfUserMentions=new HashMap<Long, Integer>();


			while(line!=null){
				JSONObject jsnobject = new JSONObject(line);
				Object timeStamp = jsnobject.get("timestamp_ms");
				long current = Long.parseLong(timeStamp.toString());
				if (current-start< windowMinutesLength*1000){
					JSONObject jsonEntities =(JSONObject)jsnobject.get("entities");
					JSONArray jsonMentionArray=jsonEntities.getJSONArray("user_mentions");
					for (int i = 0; i < jsonMentionArray.length(); i++) {
						JSONObject explrObject = jsonMentionArray.getJSONObject(i);
						String mentionedUser = explrObject.get("id").toString();
						int p=0;
						for(p=0;p<monitoredIds.length;p++)
						{
							if(monitoredIds[p]==Long.parseLong(mentionedUser))
								break;
						}
						if(p==monitoredIds.length) continue;
						Object numberOfMentions = mapOfUserMentions.get(mentionedUser);
						if(numberOfMentions!=null){
							mapOfUserMentions.put(Long.parseLong(mentionedUser),Integer.parseInt(numberOfMentions.toString())+1);
						}else{
							mapOfUserMentions.put(Long.parseLong(mentionedUser),1);
						}
					}
				}else
				{
					bw.write(start+" TO "+current +" Window : "+mapOfUserMentions.toString()+"\n");	
					start=current;
					//System.out.println(mapOfUserMentions.toString());						
					windows.add((HashMap<Long,Integer>)mapOfUserMentions.clone());						
					mapOfUserMentions.clear();
					continue;
				}
				line=br.readLine();
			}
			windows.add((HashMap<Long,Integer>)mapOfUserMentions.clone());
			br.close();
			bw.flush();
			bw.close();
			return windows;

		}catch(Exception e){e.printStackTrace(); return null;}

	}

	public ArrayList<ArrayList<HashMap<Long, Integer>>> extractSlides(int windowMinutesLength, int slideMinutesLength, String StreamFile){		
		try{
			OutputStream    fos;
			BufferedWriter bw;
			fos=new FileOutputStream(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"Debug/twitterMentionSlides.txt");
			bw=new BufferedWriter(new OutputStreamWriter(fos));
			InputStream    fis;
			BufferedReader br;

			//FIXME: remove the dependency to QueryProcessor
			long Wstart=Config.INSTANCE.getQueryStartingTime();
			long Sstart=Wstart;

			fis = new FileInputStream(StreamFile);//"D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/followerSnapshotsFile2.txt");
			br = new BufferedReader(new InputStreamReader(fis));
			String line = br.readLine();

			//ArrayList<HashMap<String,Integer>> windows=new ArrayList<HashMap<String,Integer>>();
			Queue<HashMap<Long ,Integer>> mapOfUserMentions=new LinkedList<HashMap<Long,Integer>>();
			HashMap<Long ,Integer> slideMapOfUserMention = new 	HashMap<Long ,Integer>();	
			
			Queue<HashMap<Long ,Long>> mapOfUserMentionsTime=new LinkedList<HashMap<Long,Long>>();
			HashMap<Long ,Long> slideMapOfUserMentionTime = new 	HashMap<Long ,Long>();	
			
			
			Queue<Long> slideStarts=new LinkedList<Long>();

			HashMap<Long,Long> currentWindowUsersTimestamp=new HashMap<Long, Long>();

			while(line!=null){//iterating through each tweet in the stream to put it in the right slide of the right window
				JSONObject jsnobject = new JSONObject(line);
				Object timeStamp = jsnobject.get("timestamp_ms");
				long current = Long.parseLong(timeStamp.toString());
				if (current-Wstart< windowMinutesLength*1000){//checking window boundaries => iterating over windows

					if (current - Sstart < slideMinutesLength*1000){//checking slide boundaries=>iterating over slides
						JSONObject jsonEntities =(JSONObject)jsnobject.get("entities");
						JSONArray jsonMentionArray=jsonEntities.getJSONArray("user_mentions");
						for (int i = 0; i < jsonMentionArray.length(); i++) {
							JSONObject explrObject = jsonMentionArray.getJSONObject(i);
							String mentionedUser = explrObject.get("id").toString();
							int p=0;
							for(p=0;p<monitoredIds.length;p++)
							{
								if(monitoredIds[p]==Long.parseLong(mentionedUser))
									break;
							}
							if(p==monitoredIds.length) continue;
							Integer x = userMentionCount.get(Long.parseLong(mentionedUser));
							if(x==null)
								userMentionCount.put(Long.parseLong(mentionedUser), 1);
							else
								userMentionCount.put(Long.parseLong(mentionedUser), x+1);
							
							Object numberOfMentions = slideMapOfUserMention.get(mentionedUser);
							if(numberOfMentions!=null){
								slideMapOfUserMention.put(Long.parseLong(mentionedUser),Integer.parseInt(numberOfMentions.toString())+1);
							}else{
								slideMapOfUserMention.put(Long.parseLong(mentionedUser),1);
							}
							slideMapOfUserMentionTime.put(Long.parseLong(mentionedUser), current);
						}
					}
					else{//end of current slide. adding the current slide and setting varaibles for the next slide
						bw.write(Sstart+" to "+current+" slide"+slideMapOfUserMention.toString()+"\n");
						bw.write("time : "+slideMapOfUserMentionTime.toString()+"\n");
						Sstart+=Config.INSTANCE.getQueryWindowSlide()*1000;//setting the start time of the next slide
						slideStarts.add(Sstart);//adding the slides start time for sliding windows
						mapOfUserMentions.add((HashMap<Long, Integer>)slideMapOfUserMention.clone());//adding the copy of current slide to the current window	
						mapOfUserMentionsTime.add((HashMap<Long, Long>)slideMapOfUserMentionTime.clone());//adding the copy of current slide to the current window
						slideMapOfUserMention.clear();//clearing the current slide to be filled again from stream
						slideMapOfUserMentionTime.clear();//clearing the current slide to be filled again from stream
						continue;
					}

				}else
				{//end of current window . adding the current window and setting variables for the next window 
					Wstart=slideStarts.poll();//setting the start time of the next window
					//System.out.println(mapOfUserMentions.toString());	
					mapOfUserMentions.add((HashMap<Long, Integer>)slideMapOfUserMention.clone());//adding the current slide to the slides of the current window
					mapOfUserMentionsTime.add((HashMap<Long, Long>)slideMapOfUserMentionTime.clone());//adding the current slide to the slides of the current window
					bw.write(Wstart+" TO "+current +" Window : "+mapOfUserMentions.toString()+"\n");
					bw.write("time : "+mapOfUserMentionsTime.toString()+"\n");
					windowsWithSlideEntries.add(new ArrayList<HashMap<Long,Integer>>(mapOfUserMentions));//adding the current window to the list of windows with slided entries						
					slidedWindowUsersTimeStamp.add(new ArrayList<HashMap<Long,Long>>(mapOfUserMentionsTime));//adding the list of user-entrance-timestamp for current window
					HashMap<Long,Integer> evictedUsers = mapOfUserMentions.poll();//evict the first slide from the slides of current window
					HashMap<Long,Integer> partialLastSlideEntries = ((LinkedList<HashMap<Long,Integer>>)mapOfUserMentions).removeLast();//to remove the partially added slide because it will be added fully in the next iteration
					HashMap<Long,Long> evictedUsersTime = mapOfUserMentionsTime.poll();//evict the first slide from the slides of current window
					HashMap<Long,Long> partialLastSlideEntriesTime = ((LinkedList<HashMap<Long,Long>>)mapOfUserMentionsTime).removeLast();//to remove the partially added slide because it will be added fully in the next iteration
					//evicted users should be evicted from the list of user-entrance-timestamps for the current window to be used for the next sliding window
					Iterator<Long> evictedUserIt=evictedUsers.keySet().iterator();
					while(evictedUserIt.hasNext()){
						long euid=evictedUserIt.next();
						boolean flage=false;
						for(HashMap<Long,Integer> mapOfUserMentionsSlide : mapOfUserMentions ){
							if (mapOfUserMentionsSlide.get(euid)!=null)//iterate through all slides
							{
								flage=true;
								break;
							}
						}
						if(!flage) currentWindowUsersTimestamp.remove(euid);
					}
						
					/*Iterator<Long> currentWindowUsersTimestampIt=currentWindowUsersTimestamp.keySet().iterator();
					while(currentWindowUsersTimestampIt.hasNext()){
						Long next = currentWindowUsersTimestampIt.next();
						if(partialLastSlideEntries.get(next)==null)
						currentWindowUsersTimestamp.remove(next);
					}*/
										
					continue;
				}
				line=br.readLine();
			}
			mapOfUserMentions.add((HashMap<Long, Integer>)slideMapOfUserMention.clone());
			windowsWithSlideEntries.add(new ArrayList<HashMap<Long,Integer>>(mapOfUserMentions));						
			//slidedWindowUsersTimeStamp.add((HashMap<Long,Long>)currentWindowUsersTimestamp.clone());
			mapOfUserMentionsTime.add((HashMap<Long, Long>)slideMapOfUserMentionTime.clone());
			slidedWindowUsersTimeStamp.add(new ArrayList<HashMap<Long,Long>>(mapOfUserMentionsTime));						
			//slidedWindowUsersTimeStamp.add((HashMap<Long,Long>)currentWindowUsersTimestamp.clone());
			br.close();
			bw.flush();
			bw.close();
			return windowsWithSlideEntries;

		}catch(Exception e){e.printStackTrace(); return null;}

	}


	public ArrayList<HashMap<Long,Integer>> aggregateSildedWindowsUser()
	{
		ArrayList<HashMap<Long,Integer>> slidedWindows=new ArrayList<HashMap<Long,Integer>>();
		for(int i=0;i<windowsWithSlideEntries.size();i++){
			ArrayList<HashMap<Long,Integer>> tempSplittedWindow = windowsWithSlideEntries.get(i);
			HashMap<Long,Integer> WindowUserMention=new HashMap<Long, Integer>();
			for(int j=0;j<tempSplittedWindow.size();j++){//per slide
				HashMap<Long,Integer> slideUsers=tempSplittedWindow.get(j);
				Iterator<Long> slideuserit=slideUsers.keySet().iterator();
				while(slideuserit.hasNext()){
					long slideUserName=slideuserit.next();
					Integer windowusercount= WindowUserMention.get(slideUserName);
					if(windowusercount==null){
						WindowUserMention.put(slideUserName, slideUsers.get(slideUserName));
					}else{
						WindowUserMention.put(slideUserName, slideUsers.get(slideUserName)+windowusercount);
					}
				}				
			}
			slidedWindows.add(WindowUserMention);
		}
		try{
			HashMap<Long,Integer> c= TwitterFollowerCollector.getchangecount();
			BufferedWriter bw1=new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"Debug/userMentionCount.csv")));
		Iterator<Long> it1= userMentionCount.keySet().iterator();
		while(it1.hasNext()){
			Long idtemp=it1.next();
			bw1.write(idtemp+","+userMentionCount.get(idtemp)+","+c.get(idtemp)+"\n");
		}
		bw1.flush();
		bw1.close();
		}catch(Exception e){}
		
		
		return slidedWindows;
	}
	public ArrayList<HashMap<Long,Long>> aggregateSildedWindowsUserTime()
	{
		ArrayList<HashMap<Long,Long>> slidedWindowsTime=new ArrayList<HashMap<Long,Long>>();
		for(int i=0;i<slidedWindowUsersTimeStamp.size();i++){
			ArrayList<HashMap<Long,Long>> tempSplittedWindowTime = slidedWindowUsersTimeStamp.get(i);
			HashMap<Long,Long> WindowUserMentionTime=new HashMap<Long, Long>();
			for(int j=0;j<tempSplittedWindowTime.size();j++){//per slide
				HashMap<Long,Long> slideUsersTime=tempSplittedWindowTime.get(j);
				Iterator<Long> slideuserit=slideUsersTime.keySet().iterator();
				while(slideuserit.hasNext()){
					long slideUserName=slideuserit.next();
					Long windowusertime= WindowUserMentionTime.get(slideUserName);
					WindowUserMentionTime.put(slideUserName, slideUsersTime.get(slideUserName));					
				}				
			}
			slidedWindowsTime.add(WindowUserMentionTime);
		}
		return slidedWindowsTime;
	}
	
	public Integer getMaximumUserMentions(ArrayList<HashMap<Long,Integer>> slidedWindows){
		
		Integer maxMentions = Integer.MIN_VALUE ;
		
		for ( int i = 0 ; i < slidedWindows.size() ; i++){
			
			HashMap<Long,Integer> userMentions = slidedWindows.get(i);
			Iterator<Long> it= userMentions.keySet().iterator();
			while(it.hasNext()){
				Long id=it.next();
				Integer mentions = userMentions.get(id);
				if (mentions > maxMentions){
					maxMentions = mentions ;
				}
			}
		}
		return maxMentions;
	}
	
	public Integer getMinimumUserMentions(ArrayList<HashMap<Long,Integer>> slidedWindows){
		
		Integer minMentions = Integer.MAX_VALUE ;
		
		for ( int i = 0 ; i < slidedWindows.size() ; i++){
			
			HashMap<Long,Integer> userMentions = slidedWindows.get(i);
			Iterator<Long> it= userMentions.keySet().iterator();
			while(it.hasNext()){
				Long id=it.next();
				Integer mentions = userMentions.get(id);
				if (mentions < minMentions){
					minMentions = mentions ;
				}
			}
		}
		return minMentions;
	}
	
	public static void main(String[] args)
	{
		TwitterStreamCollector tsc=new TwitterStreamCollector();
		//tsc.listen("D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/twitterStream.txt");
		//ArrayList<HashMap<Long, Integer>> windows = tsc.extractWindow(Config.INSTANCE.getQueryWindowWidth(), "D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/twitterStream.txt");
		//windows.get(1);
	}

}
