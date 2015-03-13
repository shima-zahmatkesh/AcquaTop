import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
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
		try{
			for (double a=0;a<1;a+=0.01){
			int sum=0;
			BufferedReader fr=new BufferedReader(new FileReader(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"Debug/SlidingOETJoinOperatorSSestimationErrorPerWindow.txt")));
			String line=null;
			fr.readLine();
			while((line = fr.readLine())!=null){
				String[] parts=line.split(",");
				double J = Double.parseDouble(parts[0])/Double.parseDouble(parts[3]);
				double K = Double.parseDouble(parts[1])/Double.parseDouble(parts[3]);
				if((J>a && K<a) || (J<a && K>a) ){
					sum++;
				}
				}
			System.out.println(sum);
			fr.close();
			}
		}catch(Exception e){
			
		}
	}*/
	
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
		Connection c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());
		Random r=new Random(System.currentTimeMillis());
		
		
		Statement stmt0 = c.createStatement();
		/*stmt0.executeUpdate("Drop table IF EXISTS User");
		stmt0.executeUpdate("create table User ( USERID BIGINT, CHANGERATE real);");
		String test="select distinct(bkg.USERID) as USERID from bkg";
		stmt0.close();
		Statement stmt1 = c.createStatement();
		ResultSet x= stmt1.executeQuery(test);
		while(x.next()){
			double cr = (double)Math.round(Math.random() * 1000) / 1000;
			//double cr = 1-( Math.round((Math.max(1, Math.min(100, (int) 75 + r.nextGaussian() * 25)))*1000/1000)/(double)100);
			//cr=(double)(cr*1000)/1000;
			cr=r.nextDouble();
			if (cr>0.5)
				cr=0.05;
			else cr=0.3;
			System.out.println(cr);
			test="INSERT INTO User VALUES("+x.getLong("USERID")+", "+cr+");";
			stmt0.executeUpdate(test);
		}
		x.close();
		stmt0.close();*/
		Statement stmt2 = c.createStatement();
		Statement stmt3 = c.createStatement();
		String sql="select * from User";
		ResultSet rs=stmt0.executeQuery(sql);
		while ( rs.next() ) {
			long userId = rs.getLong("USERID");
			double changeRate  =  rs.getFloat("CHANGERATE");
			int changeCount=(int)(1/changeRate);
			int count=0;
			int followerCount=r.nextInt(6000);
			String sql2="select bkg.TIMESTAMP as time from BKG where USERID="+userId;
			ResultSet times= stmt2.executeQuery(sql2);
			while(times.next())
			{
				if (changeCount!=0 && count%changeCount==0){
					followerCount=r.nextInt(6000);
					}else;
				long newTime=count*60000+Config.INSTANCE.getQueryStartingTime();
				sql="update bkg set foLLOWERCOUNT="+followerCount+",TIMESTAMP="+newTime+" where USERID="+userId+" and TIMESTAMP="+times.getLong("time");	
				System.out.println("user id>"+userId+" time>"+newTime+">>"+stmt3.executeUpdate(sql));
				count++;
			}
			System.out.println(count);
			times.close();
		}
		
		rs.close();
		
		//stmt1.close();
		stmt2.close();
		stmt3.close();
		
		}catch(Exception e){e.printStackTrace();}
		
	}
	/*public static void main(String[] args){
		try{
		Class.forName("org.sqlite.JDBC");
		Connection c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());
		
		
		Statement stmt0 = c.createStatement();
		Statement stmt3 = c.createStatement();
		String sql="select * from User";
		ResultSet rs=stmt0.executeQuery(sql);
		while ( rs.next() ) {
			long userId = rs.getLong("USERID");
			Statement stmt1=c.createStatement();
			int count=0;
			String sql2="select distinct(bkg.TIMESTAMP) as time from BKG where USERID="+userId+" order by bkg.TIMESTAMP ";
			ResultSet times= stmt1.executeQuery(sql2);
			while(times.next())
			{
				long newTime=count*60000+Config.INSTANCE.getQueryStartingTime();
				sql="update bkg set TIMESTAMP="+newTime+" where USERID="+userId+" and TIMESTAMP="+times.getLong("time");	
				//System.out.println(sql);
				stmt3.executeUpdate(sql);
				count++;
			}
			times.close();
			stmt1.close();
		}
		
		rs.close();
		}catch(Exception e){e.printStackTrace();}
	}*/
}
	