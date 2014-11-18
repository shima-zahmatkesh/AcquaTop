import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;

import twitter4j.FilterQuery;
import twitter4j.JSONArray;
import twitter4j.JSONObject;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.TwitterObjectFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
//import twitter4j.TwitterStream;
//import twitter4j.TwitterStreamFactory;
import twitter4j.UserMentionEntity;
import twitter4j.conf.ConfigurationBuilder;


public class twitterStreamCollector {
	private ConfigurationBuilder cb;
	public ArrayList<HashMap> windows;
	public twitterStreamCollector(){
		cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		  .setOAuthConsumerKey("4DLiYUzihQwpTrmzy8sGw")
		  .setOAuthConsumerSecret("gqIMoivaCuf1XuDVeOkPADYozc0ddV7ccxngDNSk")
		  .setOAuthAccessToken("96538292-6MuEd3YcQ1ClJVtQ9OceeOd4dlzm8ZhMeshUcTpRJ")
		  .setOAuthAccessTokenSecret("6lqQnvDKCP9sUwP8cJnZYD1iDWrvhhQXdeVWQfTImx4");
		windows=new ArrayList<HashMap>();
	}
	public void listen(String userListToMonitor, String OutputStreamFile){
		try{
			InputStream    fis;
			BufferedReader br;
			String         line;

			fis = new FileInputStream(userListToMonitor);//"D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/followers.csv");
			br = new BufferedReader(new InputStreamReader(fis));
		
		
			String br1 = br.readLine();
			String[] idStr = br1.split(",");
			final long[] monitoredIds=new long[idStr.length];
			for(int o=0;o<idStr.length;o++)
			{
				monitoredIds[o]=Long.parseLong(idStr[o]);
			}
			String br2=br.readLine();
			String[] monitorNames=br2.split(",");
			br.close();
			br = null;
			fis = null;
		
			final FileWriter fw=new FileWriter(new File(OutputStreamFile));//"D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/twitterStream.txt"));
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
	public void extractWindow(int windowMinutesLength, String StreamFile){
		try{
			OutputStream    fos;
			BufferedWriter bw;
			fos=new FileOutputStream("D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/twitterMentionWindows.txt");
			bw=new BufferedWriter(new OutputStreamWriter(fos));
			InputStream    fis;
			BufferedReader br;
			
			long start=new Long("1416074389529");
			
			fis = new FileInputStream(StreamFile);//"D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/followerSnapshotsFile2.txt");
			br = new BufferedReader(new InputStreamReader(fis));
			String line = br.readLine();
			
			ArrayList<HashMap> windows=new ArrayList<HashMap>();
			HashMap<String ,Integer> mapOfUserMentions=new HashMap<String, Integer>();
			
			
			while(line!=null){
				JSONObject jsnobject = new JSONObject(line);
				Object timeStamp = jsnobject.get("timestamp_ms");
				long current = Long.parseLong(timeStamp.toString());
				if (current-start< windowMinutesLength*1000){
					JSONObject jsonEntities =(JSONObject)jsnobject.get("entities");
					JSONArray jsonMentionArray=jsonEntities.getJSONArray("user_mentions");
					 for (int i = 0; i < jsonMentionArray.length(); i++) {
					        JSONObject explrObject = jsonMentionArray.getJSONObject(i);
					        String mentionedUser = explrObject.get("screen_name").toString();
					        Object numberOfMentions = mapOfUserMentions.get(mentionedUser);
					        if(numberOfMentions!=null){
					        	mapOfUserMentions.put(mentionedUser,Integer.parseInt(numberOfMentions.toString())+1);
					        }else{
					        	mapOfUserMentions.put(mentionedUser,1);
					        }
					}
				}else
					{
						start=current;
						System.out.println(mapOfUserMentions.toString());
						bw.write(mapOfUserMentions.toString()+"\n");
						windows.add((HashMap)mapOfUserMentions.clone());						
						mapOfUserMentions.clear();
						continue;
						}
				line=br.readLine();
			}
			br.close();
			bw.flush();
			bw.close();
			
		}catch(Exception e){e.printStackTrace();}
		
	}
	public static void main(String[] args)
	{
		twitterStreamCollector tsc=new twitterStreamCollector();
		tsc.listen("D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/followers.csv", "D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/twitterStream.txt");
		tsc.extractWindow(30, "D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/twitterStream.txt");
		tsc.windows.get(1);
	}
	
}
