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
	public ArrayList<HashMap<Long, Integer>> windows;
	public ArrayList<ArrayList<HashMap<Long,Integer>>> slides;
	public static long[] monitoredIds;
	public static String[] monitorNames;
	//constructor for not listening
	/*public TwitterStreamCollector(){
		windows=new ArrayList<HashMap<Long,Integer>>();
		extractUserIds("D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/followers.init");
	}*/

	//constructor for listening
	public TwitterStreamCollector(){
		cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		  .setOAuthConsumerKey("4DLiYUzihQwpTrmzy8sGw")
		  .setOAuthConsumerSecret("gqIMoivaCuf1XuDVeOkPADYozc0ddV7ccxngDNSk")
		  .setOAuthAccessToken("96538292-6MuEd3YcQ1ClJVtQ9OceeOd4dlzm8ZhMeshUcTpRJ")
		  .setOAuthAccessTokenSecret("6lqQnvDKCP9sUwP8cJnZYD1iDWrvhhQXdeVWQfTImx4");
		cb.setJSONStoreEnabled(true);
		windows= new ArrayList<HashMap<Long,Integer>>();
		slides=new ArrayList<ArrayList<HashMap<Long,Integer>>>();
		extractUserIds("D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/followers.init");	
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
	public ArrayList<HashMap<Long, Integer>> extractWindow(int windowMinutesLength, String StreamFile){		
		try{
			OutputStream    fos;
			BufferedWriter bw;
			fos=new FileOutputStream("D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/twitterMentionWindows.txt");
			bw=new BufferedWriter(new OutputStreamWriter(fos));
			InputStream    fis;
			BufferedReader br;

			//FIXME: remove the dependency to QueryProcessor
			long start=Config.INSTANCE.getQueryStartingTime();

			fis = new FileInputStream(StreamFile);//"D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/followerSnapshotsFile2.txt");
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
					System.out.println(mapOfUserMentions.toString());						
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
			fos=new FileOutputStream("D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/twitterMentionWindows.txt");
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
			Queue<Long> slideStarts=new LinkedList<Long>();

			while(line!=null){
				JSONObject jsnobject = new JSONObject(line);
				Object timeStamp = jsnobject.get("timestamp_ms");
				long current = Long.parseLong(timeStamp.toString());
				if (current-Wstart< windowMinutesLength*1000){//iterating over windows
					
					if (current - Sstart < slideMinutesLength*1000){//iterating over slides
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
							Object numberOfMentions = slideMapOfUserMention.get(mentionedUser);
							if(numberOfMentions!=null){
								slideMapOfUserMention.put(Long.parseLong(mentionedUser),Integer.parseInt(numberOfMentions.toString())+1);
							}else{
								slideMapOfUserMention.put(Long.parseLong(mentionedUser),1);
							}
						}
					}//end of slide
					else{
						bw.write(Sstart+" to "+current+" slide"+slideMapOfUserMention.toString()+"\n");
						Sstart=current;
						slideStarts.add(Sstart);
						mapOfUserMentions.add((HashMap<Long, Integer>)slideMapOfUserMention.clone());	
						slideMapOfUserMention.clear();
						continue;
					}
					
				}else
				{
					Wstart=slideStarts.poll();
					//System.out.println(mapOfUserMentions.toString());	
					mapOfUserMentions.add((HashMap<Long, Integer>)slideMapOfUserMention.clone());
					bw.write(Wstart+" TO "+current +" Window : "+mapOfUserMentions.toString()+"\n");	
					slides.add(new ArrayList<HashMap<Long,Integer>>(mapOfUserMentions));						
					mapOfUserMentions.poll();
					//slideMapOfUserMention.clear();
					continue;
				}
				line=br.readLine();
			}
			mapOfUserMentions.add((HashMap<Long, Integer>)slideMapOfUserMention.clone());
			slides.add(new ArrayList<HashMap<Long,Integer>>(mapOfUserMentions));						
			
			br.close();
			bw.flush();
			bw.close();
			return slides;

		}catch(Exception e){e.printStackTrace(); return null;}

	}
	public ArrayList<HashMap<Long,Integer>> aggregateSildedWindowsUser()
	{
		ArrayList<HashMap<Long,Integer>> slidedWindows=new ArrayList<HashMap<Long,Integer>>();
		for(int i=0;i<slides.size();i++){
			ArrayList<HashMap<Long,Integer>> tempSplittedWindow = slides.get(i);
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
		return slidedWindows;
	}
	public static void main(String[] args)
	{
		TwitterStreamCollector tsc=new TwitterStreamCollector();
		//tsc.listen("D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/twitterStream.txt");
		//ArrayList<HashMap<Long, Integer>> windows = tsc.extractWindow(Config.INSTANCE.getQueryWindowWidth(), "D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/twitterStream.txt");
		//windows.get(1);
	}

}
