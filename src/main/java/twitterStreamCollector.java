import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import twitter4j.FilterQuery;
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
	public static void main(String[] args)
	{
		try{
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		  .setOAuthConsumerKey("4DLiYUzihQwpTrmzy8sGw")
		  .setOAuthConsumerSecret("gqIMoivaCuf1XuDVeOkPADYozc0ddV7ccxngDNSk")
		  .setOAuthAccessToken("96538292-6MuEd3YcQ1ClJVtQ9OceeOd4dlzm8ZhMeshUcTpRJ")
		  .setOAuthAccessTokenSecret("6lqQnvDKCP9sUwP8cJnZYD1iDWrvhhQXdeVWQfTImx4")
		  .setJSONStoreEnabled(true);
		//TwitterFactory tf = new TwitterFactory(cb.build());
		//Twitter twitter = tf.getInstance();
		
		InputStream    fis;
		BufferedReader br;
		String         line;

		fis = new FileInputStream("G:/acquaProj/followers.csv");
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
		
		final FileWriter fw=new FileWriter(new File("g:/acquaProj/twitterStream.txt"));
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
		        				 System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
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
	public void parseStream(){
		InputStream    fis;
		BufferedReader br;
		String         line;
try{
		fis = new FileInputStream("G:/acquaProj/followers.csv");
		br = new BufferedReader(new InputStreamReader(fis));
}catch(IOException)
		
	}
}
