import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Random;

import acqua.config.Config;

import twitter4j.IDs;
import twitter4j.ResponseList;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.User;
import twitter4j.conf.ConfigurationBuilder;


public class Main {
	/*public static void main(String[] args) throws  Exception {
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
		  
	}*/
	public static void main(String[] args){
		try{
		Class.forName("org.sqlite.JDBC");
		Connection c = DriverManager.getConnection("jdbc:sqlite:testeveningCopy.db");
		Random r=new Random();
		
		Statement stmt1 = c.createStatement();
		Statement stmt2 = c.createStatement();
		Statement stmt3 = c.createStatement();
		String sql="select * from user";
		ResultSet rs=stmt1.executeQuery(sql);
		while ( rs.next() ) {
			long userId = rs.getLong("USERID");
			int changeCount  = rs.getInt("ChangeCount");//it changes every changeCount
			int count=0;
			int followerCount=r.nextInt(6000);
			String sql2="select distinct(bkg.tiMESTAMP) as time from BKG";
			ResultSet times= stmt2.executeQuery(sql2);
			while(times.next())
			{
				count++;
				if (count%changeCount==0){
					followerCount=r.nextInt(6000);
					}else;
				sql="update bkg set foLLOWERCOUNT="+followerCount+" where USERID="+userId+" and TIMESTAMP="+times.getLong("time");	
				stmt3.executeUpdate(sql);
			}
			times.close();
		}
		
		rs.close();
		
		stmt1.close();
		stmt2.close();
		stmt3.close();
		
		}catch(Exception e){e.printStackTrace();}
		
	}
}