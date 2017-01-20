package acqua.data.generator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.regex.Pattern;

import acqua.config.Config;

public class StreamGenerator {
	
	static HashMap<Long,Long> userNumberofFollowers = new HashMap<Long,Long> ();
	static Long initialTimestamp = 0l; 
	
	private static void StreamEventExtractor (String streamEventFilePath  ){
		
		BufferedReader br;
		//extract number of followers
		//createTables();
		try {
	
			br = new BufferedReader(new InputStreamReader(new FileInputStream(streamEventFilePath)));
			//BufferedWriter bw=new BufferedWriter(new FileWriter(new File(newPath)));			
			String line = br.readLine();
			String newLine = "";
			while(line!=null){
				System.out.println(  line);
				String[] lineSplit = line.split(Pattern.quote("|"));
				int type = Integer.parseInt(lineSplit[0]);
				switch (type) {
					case 1: extractPostLike (line);
							//System.out.println("case 1");
							break;
					case 2: extractCommentLike (line);
							//System.out.println("case 2");
							break;
					case 3: extractReplyTopPostOrComment (line);
							//System.out.println("case 3");
							break;
				}
					
				line = br.readLine();	
			}
			br.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("extranct stearm events done");
		
	}
	
	private static void createTables(){
		Connection c = null;
		Statement stmt = null;
		
		try {
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());

			stmt = c.createStatement();
			stmt.executeUpdate(" DROP TABLE IF EXISTS MENTIONS ;");
			String sql = "CREATE TABLE  `MENTIONS` ( " +
					" `TIMESTAMP`        BIGINT    NOT NULL, "+ 
					" `USERID`           BIGINT    NOT NULL, "+
					"  FOREIGN KEY (USERID) REFERENCES PERSON(USERID) ); "; 
			stmt.executeUpdate(sql);
			System.out.println("creat table done");
			stmt.close();
			c.close();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	
	private static void extractCommentLike ( String line){
		
		try {
			Class.forName("org.sqlite.JDBC");
		
			Connection c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());
			Statement stmt =  c.createStatement();
			
			
			String[] lineSplit = line.split(Pattern.quote("|"));
			int type = Integer.parseInt(lineSplit[0]);
			long ts = Long.parseLong(lineSplit[1]);
			long userId = Long.parseLong(lineSplit[2]);
			long commentId = Long.parseLong(lineSplit[3]);
			
			String sql = "SELECT USERID FROM COMMENT WHERE COMMENTID = " + commentId ;
			
			ResultSet rs = stmt.executeQuery( sql);
			long cteratorId = rs.getLong("USERID");
			rs.close();
			stmt.close();
			stmt =  c.createStatement();
			
			sql = "INSERT INTO MENTIONS (TIMESTAMP, USERID) " +
					"VALUES ("+ ts + ","+ cteratorId+")"; 
			stmt.executeUpdate(sql);
		
			stmt.close();
			c.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static void extractPostLike ( String line){
		
		try {
			Class.forName("org.sqlite.JDBC");
		
			Connection c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());
			Statement stmt =  c.createStatement();
			
			String[] lineSplit = line.split(Pattern.quote("|"));
			int type = Integer.parseInt(lineSplit[0]);
			long ts = Long.parseLong(lineSplit[1]);
			long userId = Long.parseLong(lineSplit[2]);
			long postId = Long.parseLong(lineSplit[3]);
			
			String sql = "SELECT USERID FROM POST WHERE POSTID = " + postId ;
			
			ResultSet rs = stmt.executeQuery( sql);
			long cteratorId = rs.getLong("USERID");
			rs.close();
			stmt.close();
			
			stmt =  c.createStatement();
			sql = "INSERT INTO MENTIONS (TIMESTAMP, USERID) " +
					"VALUES ("+ ts + ","+ cteratorId +")"; 
			stmt.executeUpdate(sql);
			stmt.close();
			c.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static void extractReplyTopPostOrComment ( String line){
		
		try {
			Class.forName("org.sqlite.JDBC");
		
			Connection c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());
			Statement stmt =  c.createStatement();
			
			String[] lineSplit = line.split(Pattern.quote("|"));  //3|ts|author1|comment1|comment2|-1

			int type = Integer.parseInt(lineSplit[0]);
			long ts = Long.parseLong(lineSplit[1]);
			long userId = Long.parseLong(lineSplit[2]);
			long replyId = Long.parseLong(lineSplit[3]);
			long commentId = Long.parseLong(lineSplit[4]);
			long postId = Long.parseLong(lineSplit[5]);
			
			String sql = "";
			if (commentId != -1){
				sql = "SELECT USERID FROM COMMENT WHERE COMMENTID = " + commentId ;
			}else if (postId != -1){
				sql = "SELECT USERID FROM POST WHERE POSTID = " + postId ;
			}
			
			ResultSet rs = stmt.executeQuery( sql);
			long cteratorId = rs.getLong("USERID");
			rs.close();
			stmt.close();
			
			stmt =  c.createStatement();

			sql = "INSERT INTO MENTIONS (TIMESTAMP, USERID) " +
					"VALUES ("+ ts + ","+ cteratorId +")"; 
			stmt.executeUpdate(sql);
			stmt.close();
			c.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public static void main(String[] agrs) {
		
		createTables();
		StreamEventExtractor("/Users/zahmatkesh/git/datacollector/NewData/new_stream_events.csv" );
		
	}
}
