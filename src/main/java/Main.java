import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;

import twitter4j.IDs;
import twitter4j.ResponseList;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.User;
import twitter4j.conf.ConfigurationBuilder;


public class Main {
	public static void main(String[] args) throws  Exception {
		/////////////////////////////////////////////////////////////////////////////////////////////
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
			
		FileWriter followerFile=new FileWriter(new File("D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/followerFile.txt"));
		long lCursor = -1;
		IDs friendsIDs = twitter.getFriendsIDs("verified", lCursor);
		//System.out.println(twitter.showUser("verified").getName());
		System.out.println("==========================");
		long[] ids = friendsIDs.getIDs();
		int idIndex = 0;
		for(int l = 0; l < 30; l++){
		long[] lookupIds = new long[100];
		for(int i = 0; i<100; i++){
		lookupIds[i]=ids[idIndex++];
		}
		ResponseList<User> users = twitter.lookupUsers(lookupIds);
		for(User u : users){
		     followerFile.write(u.getId()+ ",\"" +u.getScreenName()+"\","+ u.getName()+ "," +u.getFollowersCount()+","+System.currentTimeMillis()+"\n");
		  }
		}
		
		  followerFile.flush();
		  followerFile.close();
		  //////////////////////////////////////////////////////////////////
		  
	}
}