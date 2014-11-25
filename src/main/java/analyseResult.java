import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;


public class analyseResult {
public static void insertResultToDB(){
	
	Connection c = null;
	Statement stmt = null;
    try {		
	      Class.forName("org.sqlite.JDBC");
	      c = DriverManager.getConnection("jdbc:sqlite:test.db");
	      
	      stmt = c.createStatement();
	      //stmt.executeQuery("DROP INDEX IF EXISTS timeIndex ON BKG;");
	      stmt.executeUpdate(" DROP TABLE IF EXISTS BJ ;");
	      String sql = "CREATE TABLE  `BJ` ( " +
	                   " `USERID`           BIGINT    NOT NULL, " + 
	                   " `MENTIONCOUNT`     INT    NOT NULL, " + 
	                   " `FOLLOWERCOUNT`    INT    NOT NULL, " + 
	                   " `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `BtimeIndex` ON `BJ` (`TIMESTAMP` ASC);"; 
	      System.out.println(sql);
	      stmt.executeUpdate(sql);
	      stmt.executeUpdate(" DROP TABLE IF EXISTS OJ ;");
	      sql = "CREATE TABLE  `OJ` ( " +
                  " `USERID`           BIGINT    NOT NULL, " + 
                  " `MENTIONCOUNT`     INT    NOT NULL, " + 
                  " `FOLLOWERCOUNT`    INT    NOT NULL, " + 
                  " `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `OtimeIndex` ON `OJ` (`TIMESTAMP` ASC);"; 
	      System.out.println(sql);
	      stmt.executeUpdate(sql);
	      stmt.executeUpdate(" DROP TABLE IF EXISTS DWJ ;");
	      sql = "CREATE TABLE  `DWJ` ( " +
                  " `USERID`           BIGINT    NOT NULL, " + 
                  " `MENTIONCOUNT`     INT    NOT NULL, " + 
                  " `FOLLOWERCOUNT`    INT    NOT NULL, " + 
                  " `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `DWtimeIndex` ON `DWJ` (`TIMESTAMP` ASC);"; 
	      System.out.println(sql);
	      stmt.executeUpdate(sql);
	      stmt.executeUpdate(" DROP TABLE IF EXISTS RJ ;");
	      sql = "CREATE TABLE  `RJ` ( " +
                  " `USERID`           BIGINT    NOT NULL, " + 
                  " `MENTIONCOUNT`     INT    NOT NULL, " + 
                  " `FOLLOWERCOUNT`    INT    NOT NULL, " + 
                  " `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `RtimeIndex` ON `RJ` (`TIMESTAMP` ASC);"; 
	      System.out.println(sql);
	      stmt.executeUpdate(sql);
		InputStream    fis;
		BufferedReader br;
		//---------------------------------------------------------------------fill baseline table
		fis = new FileInputStream("D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/joinOutput/BaselineJoinOperatorOutput.txt");
		br = new BufferedReader(new InputStreamReader(fis));
		String line=null;
		while((line=br.readLine())!=null)
		{
			String[] userInfo = line.split(" ");	
			sql = "INSERT INTO BJ (USERID,MENTIONCOUNT,FOLLOWERCOUNT,TIMESTAMP) " +
          "VALUES ("+userInfo[0]+","+userInfo[1]+","+userInfo[2]+","+userInfo[3]+")"; 
			//System.out.println(sql);
			stmt.executeUpdate(sql);
		}
		//---------------------------------------------------------------------fill DWJ
		fis = new FileInputStream("D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/joinOutput/DWJoinOperatorOutput.txt");
		br = new BufferedReader(new InputStreamReader(fis));
		line=null;
		while((line=br.readLine())!=null)
		{
			String[] userInfo = line.split(" ");	
			sql = "INSERT INTO DWJ (USERID,MENTIONCOUNT,FOLLOWERCOUNT,TIMESTAMP) " +
          "VALUES ("+userInfo[0]+","+userInfo[1]+","+userInfo[2]+","+userInfo[3]+")"; 
			//System.out.println(sql);
			stmt.executeUpdate(sql);
		}
		//-----------------------------------------------------------------------fill classqueryProcessorOracleJoinOperatorOutput
		fis = new FileInputStream("D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/joinOutput/OracleJoinOperatorOutput.txt");
		br = new BufferedReader(new InputStreamReader(fis));
		line=null;
		while((line=br.readLine())!=null)
		{
			String[] userInfo = line.split(" ");	
			sql = "INSERT INTO OJ (USERID,MENTIONCOUNT,FOLLOWERCOUNT,TIMESTAMP) " +
          "VALUES ("+userInfo[0]+","+userInfo[1]+","+userInfo[2]+","+userInfo[3]+")"; 
			//System.out.println(sql);
			stmt.executeUpdate(sql);
		}
		//-----------------------------------------------------------------------fill classqueryProcessorOracleJoinOperatorOutput
				fis = new FileInputStream("D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/joinOutput/randomCacheUpdateJoinOutput.txt");
				br = new BufferedReader(new InputStreamReader(fis));
				line=null;
				while((line=br.readLine())!=null)
				{
					String[] userInfo = line.split(" ");	
					sql = "INSERT INTO RJ (USERID,MENTIONCOUNT,FOLLOWERCOUNT,TIMESTAMP) " +
		          "VALUES ("+userInfo[0]+","+userInfo[1]+","+userInfo[2]+","+userInfo[3]+")"; 
					//System.out.println(sql);
					stmt.executeUpdate(sql);
				}
		stmt.close();
		//c.commit();
		c.close();
		br.close();
		
	
}catch(Exception e){e.printStackTrace();}
}
public static HashMap<Long,Integer> computeBJoinPrecision(){
	HashMap<Long,Integer> result=new HashMap<Long, Integer>();
	Connection c = null;
	Statement stmt = null;
    try {		
	      Class.forName("org.sqlite.JDBC");
	      c = DriverManager.getConnection("jdbc:sqlite:test.db");	      
	      c.setAutoCommit(false);
	      stmt = c.createStatement();
	      String sql="SELECT BJ.TIMESTAMP TIME, SUM(ABS(BJ.FOLLOWERCOUNT -OJ.FOLLOWERCOUNT )) Error "+
	    		  " FROM BJ,OJ  WHERE BJ.USERID=OJ.USERID AND BJ.TIMESTAMP=OJ.TIMESTAMP group by BJ.TIMESTAMP";
	      //System.out.println(sql);
	      ResultSet rs = stmt.executeQuery( sql);
	      
	      while ( rs.next() ) {
	    	  int error  = rs.getInt("Error");
	         long timeStamp  = rs.getLong("TIME");
	         result.put(timeStamp,error);
	      }
	      rs.close();
	      stmt.close();
	      c.close();
    }catch(Exception e){e.printStackTrace();}
    return result;
}
public static HashMap<Long,Integer> computeDWJoinPrecision(){
	HashMap<Long,Integer> result=new HashMap<Long, Integer>();
	Connection c = null;
	Statement stmt = null;
    try {		
	      Class.forName("org.sqlite.JDBC");
	      c = DriverManager.getConnection("jdbc:sqlite:test.db");	      
	      c.setAutoCommit(false);
	      stmt = c.createStatement();
	      //String sql="SELECT DWJ.TIMESTAMP AS TIME, SUM(ABS(DWJ.FOLLOWERCOUNT-OJ.FOLLOWERCOUNT)) AS ERROR "+
	      //		  " FROM DWJ,OJ  WHERE DWJ.USERID=OJ.USERID AND DWJ.TIMESTAMP=OJ.TIMESTAMP group by OJ.TIMESTAMP";
	      String sql="SELECT DWJ.TIMESTAMP TIME, COUNT(*) ERROR FROM DWJ,OJ  WHERE DWJ.USERID=OJ.USERID AND DWJ.TIMESTAMP=OJ.TIMESTAMP AND DWJ.FOLLOWERCOUNT <> OJ.FOLLOWERCOUNT group by OJ.TIMESTAMP";
	      //System.out.println(sql);
	      ResultSet rs = stmt.executeQuery( sql);
	      
	      while ( rs.next() ) {
	    	  int error  = rs.getInt("Error");
		      long timeStamp  = rs.getLong("TIME");
		      result.put(timeStamp,error);
	      }
	      rs.close();
	      stmt.close();
	      c.close();
    }catch(Exception e){e.printStackTrace();}
    return result;
}
public static void main(String[] args){
	insertResultToDB();
	//select timestamp, COUNT(*) from OJ  group by timestamp
	HashMap<Long,Integer> DWError=computeDWJoinPrecision();
	HashMap<Long,Integer> BError=computeBJoinPrecision();
	Iterator<Long> it = DWError.keySet().iterator();
	Iterator<Long> itB=BError.keySet().iterator();
	while(it.hasNext()){
		long nextTime = it.next();
		System.out.println(nextTime+" "+DWError.get(nextTime));
	}
	System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
	while(itB.hasNext()){
		long nextTime = itB.next();
		System.out.println(nextTime+" "+BError.get(nextTime));
	}
}
}
