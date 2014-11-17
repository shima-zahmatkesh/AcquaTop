import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

import twitter4j.IDs;
import twitter4j.ResponseList;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.User;
import twitter4j.conf.ConfigurationBuilder;


public class TwitterFollowerCollector {
public static void main(String[] args){
	ConfigurationBuilder cb = new ConfigurationBuilder();
	cb.setDebugEnabled(true)
	  .setOAuthConsumerKey("4DLiYUzihQwpTrmzy8sGw")
	  .setOAuthConsumerSecret("gqIMoivaCuf1XuDVeOkPADYozc0ddV7ccxngDNSk")
	  .setOAuthAccessToken("96538292-6MuEd3YcQ1ClJVtQ9OceeOd4dlzm8ZhMeshUcTpRJ")
	  .setOAuthAccessTokenSecret("6lqQnvDKCP9sUwP8cJnZYD1iDWrvhhQXdeVWQfTImx4");
	TwitterFactory tf = new TwitterFactory(cb.build());
	Twitter twitter = tf.getInstance();
	int numberOfUsers=100;
	ArrayList<String> userList=null;
		try{
	FileWriter followerFile=new FileWriter(new File("D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/followerSnapshotsFile2.txt"));
	/////////////////////////////////////////////////////
	InputStream    fis;
	BufferedReader br;
	
	fis = new FileInputStream("D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/followers.csv");
	br = new BufferedReader(new InputStreamReader(fis));
	
	
	String br1 = br.readLine();
	String[] idStr = br1.split(",");
	final long[] monitoredIds=new long[idStr.length];
	for(int o=0;o<idStr.length;o++)
	{
		monitoredIds[o]=Long.parseLong(idStr[o]);
	}
	br.close();
	br = null;
	fis = null;
	
	for(int y=0;y<30;y++){
		ResponseList<User> users = twitter.lookupUsers(monitoredIds);
			for(User u : users){
				//Object followerCountList = (ArrayList<Integer>)table.get(u.getScreenName());
				//if(followerCountList==null){table.put(u.getScreenName(), arg1)}
				followerFile.write(u.getId()+ ",\"" +u.getScreenName()+"\","+ u.getName()+ "," +u.getFollowersCount()+","+System.currentTimeMillis()+"\n");
			}
		
		followerFile.flush();
		Thread.sleep(1000*60*1);//sleep for 1 minutes
	}
	
	  followerFile.close();
		}catch(Exception e){e.printStackTrace();}	
}
}
