package acqua.query.result;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;


public class ResultAnalyser {
public static void insertResultToDB(){
	
	Connection c = null;
	Statement stmt = null;
    try {		
	      Class.forName("org.sqlite.JDBC");
	      c = DriverManager.getConnection("jdbc:sqlite:testevening.db");
	      
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
	      stmt.executeUpdate(" DROP TABLE IF EXISTS SJ ;");
	      sql = "CREATE TABLE  `SJ` ( " +
                  " `USERID`           BIGINT    NOT NULL, " + 
                  " `MENTIONCOUNT`     INT    NOT NULL, " + 
                  " `FOLLOWERCOUNT`    INT    NOT NULL, " + 
                  " `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `StimeIndex` ON `SJ` (`TIMESTAMP` ASC);"; 
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
				//-----------------------------------------------------------------------fill classqueryProcessorOracleJoinOperatorOutput
				fis = new FileInputStream("D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/joinOutput/SmartJoinOutput.txt");
				br = new BufferedReader(new InputStreamReader(fis));
				line=null;
				while((line=br.readLine())!=null)
				{
					String[] userInfo = line.split(" ");	
					sql = "INSERT INTO SJ (USERID,MENTIONCOUNT,FOLLOWERCOUNT,TIMESTAMP) " +
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

// OJ user cardinality per time stamp = SELECT oj.TIMESTAMP, count(OJ.USERID) FROM OJ  group by OJ.TIMESTAMP

public static HashMap<Long,Integer> computeOJoin(){
	HashMap<Long,Integer> result=new HashMap<Long, Integer>();
	Connection c = null;
	Statement stmt = null;
    try {		
	      Class.forName("org.sqlite.JDBC");
	      c = DriverManager.getConnection("jdbc:sqlite:testevening.db");	      
	      c.setAutoCommit(false);
	      stmt = c.createStatement();
	      String sql="SELECT oj.TIMESTAMP as TIMESTAMP, count(OJ.USERID) as windowcount FROM OJ  group by OJ.TIMESTAMP";      //System.out.println(sql);
	      ResultSet rs = stmt.executeQuery( sql);
	      
	      while ( rs.next() ) {
	    	  Integer error  = rs.getInt("windowcount");
	         Long timeStamp  = rs.getLong("TIMESTAMP");
	         if (error==null) error=0;
	         result.put(timeStamp,error);
	      }
	      rs.close();
	      stmt.close();
	      c.close();
    }catch(Exception e){e.printStackTrace();}
    return result;
}//--select X.TIMESTAMP,X.ERROR from (SELECT OJ.TIMESTAMP FROM OJ group by OJ.TIMESTAMP) as Y LEFT Outer join (SELECT SJ.TIMESTAMP as TIMESTAMP , COUNT(*) as ERROR FROM SJ,OJ  WHERE SJ.USERID=OJ.USERID AND SJ.TIMESTAMP=OJ.TIMESTAMP AND SJ.FOLLOWERCOUNT <> OJ.FOLLOWERCOUNT group by OJ.TIMESTAMP) as X on X.TIMESTAMP=Y.TIMESTAMP
public static HashMap<Long,Integer> computeBJoinPrecision(){
	HashMap<Long,Integer> result=new HashMap<Long, Integer>();
	Connection c = null;
	Statement stmt = null;
    try {		
	      Class.forName("org.sqlite.JDBC");
	      c = DriverManager.getConnection("jdbc:sqlite:testevening.db");	      
	      c.setAutoCommit(false);
	      stmt = c.createStatement();
	      String sql="SELECT BJ.TIMESTAMP as TIMESTAMP , COUNT(*) as ERROR FROM BJ,OJ  WHERE BJ.USERID=OJ.USERID AND BJ.TIMESTAMP=OJ.TIMESTAMP AND BJ.FOLLOWERCOUNT <> OJ.FOLLOWERCOUNT group by OJ.TIMESTAMP";
	      //System.out.println(sql);
	      ResultSet rs = stmt.executeQuery( sql);
	      
	      while ( rs.next() ) {
	    	  Integer error  = rs.getInt("ERROR");
	         Long timeStamp  = rs.getLong("TIMESTAMP");
	         if (error==null) 
	        	 error=0;
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
	      c = DriverManager.getConnection("jdbc:sqlite:testevening.db");	      
	      c.setAutoCommit(false);
	      stmt = c.createStatement();
	      //String sql="SELECT DWJ.TIMESTAMP AS TIME, SUM(ABS(DWJ.FOLLOWERCOUNT-OJ.FOLLOWERCOUNT)) AS ERROR "+
	      //		  " FROM DWJ,OJ  WHERE DWJ.USERID=OJ.USERID AND DWJ.TIMESTAMP=OJ.TIMESTAMP group by OJ.TIMESTAMP";
	      String sql="SELECT DWJ.TIMESTAMP as TIMESTAMP , COUNT(*) as ERROR FROM DWJ,OJ  WHERE DWJ.USERID=OJ.USERID AND DWJ.TIMESTAMP=OJ.TIMESTAMP AND DWJ.FOLLOWERCOUNT <> OJ.FOLLOWERCOUNT group by OJ.TIMESTAMP";
		     
	      ResultSet rs = stmt.executeQuery( sql);
	      
	      while ( rs.next() ) {
	    	  Integer error  = rs.getInt("ERROR");
		      Long timeStamp  = rs.getLong("TIMESTAMP");
		      if(error==null)
		    	  error=0;
		      result.put(timeStamp,error);
	      }
	      rs.close();
	      stmt.close();
	      c.close();
    }catch(Exception e){e.printStackTrace();}
    return result;
}
public static HashMap<Long,Integer> computeRJoinPrecision(){
	HashMap<Long,Integer> result=new HashMap<Long, Integer>();
	Connection c = null;
	Statement stmt = null;
    try {		
	      Class.forName("org.sqlite.JDBC");
	      c = DriverManager.getConnection("jdbc:sqlite:testevening.db");	      
	      c.setAutoCommit(false);
	      stmt = c.createStatement();
	      //String sql="SELECT DWJ.TIMESTAMP AS TIME, SUM(ABS(DWJ.FOLLOWERCOUNT-OJ.FOLLOWERCOUNT)) AS ERROR "+
	      //		  " FROM DWJ,OJ  WHERE DWJ.USERID=OJ.USERID AND DWJ.TIMESTAMP=OJ.TIMESTAMP group by OJ.TIMESTAMP";
	      String sql="SELECT RJ.TIMESTAMP as TIMESTAMP , COUNT(*) as ERROR FROM RJ,OJ  WHERE RJ.USERID=OJ.USERID AND RJ.TIMESTAMP=OJ.TIMESTAMP AND RJ.FOLLOWERCOUNT <> OJ.FOLLOWERCOUNT group by OJ.TIMESTAMP";
		     
	      ResultSet rs = stmt.executeQuery( sql);
	      
	      while ( rs.next() ) {
	    	  Integer error  = rs.getInt("ERROR");
		      Long timeStamp  = rs.getLong("TIMESTAMP");
		      if(error==null) 
		    	  error=0;
		      result.put(timeStamp,error);
	      }
	      rs.close();
	      stmt.close();
	      c.close();
    }catch(Exception e){e.printStackTrace();}
    return result;
}
public static HashMap<Long,Integer> computeSJoinPrecision(){
	HashMap<Long,Integer> result=new HashMap<Long, Integer>();
	Connection c = null;
	Statement stmt = null;
    try {		
	      Class.forName("org.sqlite.JDBC");
	      c = DriverManager.getConnection("jdbc:sqlite:testevening.db");	      
	      c.setAutoCommit(false);
	      stmt = c.createStatement();
	      //String sql="SELECT DWJ.TIMESTAMP AS TIME, SUM(ABS(DWJ.FOLLOWERCOUNT-OJ.FOLLOWERCOUNT)) AS ERROR "+
	      //		  " FROM DWJ,OJ  WHERE DWJ.USERID=OJ.USERID AND DWJ.TIMESTAMP=OJ.TIMESTAMP group by OJ.TIMESTAMP";
	      String sql="SELECT SJ.TIMESTAMP as TIMESTAMP , COUNT(*) as ERROR FROM SJ,OJ  WHERE SJ.USERID=OJ.USERID AND SJ.TIMESTAMP=OJ.TIMESTAMP AND SJ.FOLLOWERCOUNT <> OJ.FOLLOWERCOUNT group by OJ.TIMESTAMP";
		     
	      ResultSet rs = stmt.executeQuery( sql);
	      
	      while ( rs.next() ) {
	    	  Integer error  = rs.getInt("ERROR");
		      Long timeStamp  = rs.getLong("TIMESTAMP");
		      if(error==null) 
		    	  error=0;
		      result.put(timeStamp,error);
	      }
	      rs.close();
	      stmt.close();
	      c.close();
    }catch(Exception e){e.printStackTrace();}
    return result;
}
public static void main(String[] args){
	try{
	BufferedWriter bw=new BufferedWriter(new FileWriter(new File("D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/joinOutput/compare.csv")));
	insertResultToDB();
	HashMap<Long,Integer> oracleCount=computeOJoin();
	HashMap<Long,Integer> SError=computeSJoinPrecision();
	HashMap<Long,Integer> RError=computeRJoinPrecision();
	HashMap<Long,Integer> DWError=computeDWJoinPrecision();
	HashMap<Long,Integer> BError=computeBJoinPrecision();
	Iterator<Long> itO = oracleCount.keySet().iterator();
	bw.write("timestampe,DW,Smart,random,LRU \n");
	while(itO.hasNext()){
		long nextTime = itO.next();
		bw.write(nextTime+","+(DWError.get(nextTime)==null?0:DWError.get(nextTime))+","+(SError.get(nextTime)==null?0:SError.get(nextTime))+","+(RError.get(nextTime)==null?0:RError.get(nextTime))+","+(BError.get(nextTime)==null?0:BError.get(nextTime))+"\n");
	}
	bw.flush();
	bw.close();
	}catch(Exception e){e.printStackTrace();}
}
}
