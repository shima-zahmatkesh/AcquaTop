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
import java.util.TreeMap;

import acqua.config.Config;


public class ResultAnalyser {
	public static void insertResultToDB(){

		Connection c = null;
		Statement stmt = null;
		try {		
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());

			c.setAutoCommit(true); // only required if autocommit state not known
			Statement stat = c.createStatement(); 
			stat.executeUpdate("PRAGMA synchronous = OFF;");
			stat.close();

			stmt = c.createStatement();
			//stmt.executeQuery("DROP INDEX IF EXISTS timeIndex ON BKG;");
			stmt.executeUpdate(" DROP TABLE IF EXISTS LRUJ ;");
			String sql = "CREATE TABLE  `LRUJ` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `LRUtimeIndex` ON `LRUJ` (`TIMESTAMP` ASC);"; 
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
			stmt.executeUpdate(" DROP TABLE IF EXISTS LRUNLJ ;");
			sql = "CREATE TABLE  `LRUNLJ` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `LRUNLJtimeIndex` ON `LRUNLJ` (`TIMESTAMP` ASC);"; 
			System.out.println(sql);
			stmt.executeUpdate(sql);
			stmt.executeUpdate(" DROP TABLE IF EXISTS RNLJ ;");
			sql = "CREATE TABLE  `RNLJ` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `RNLJtimeIndex` ON `RNLJ` (`TIMESTAMP` ASC);"; 
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
			stmt.executeUpdate(" DROP TABLE IF EXISTS PWSJ ;");
			sql = "CREATE TABLE  `PWSJ` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `PWSJtimeIndex` ON `PWSJ` (`TIMESTAMP` ASC);"; 
			System.out.println(sql);
			stmt.executeUpdate(sql);
			stmt.executeUpdate(" DROP TABLE IF EXISTS SSJ ;");
			sql = "CREATE TABLE  `SSJ` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `SstimeIndex` ON `SSJ` (`TIMESTAMP` ASC);"; 
			System.out.println(sql);
			stmt.executeUpdate(sql);

			stmt.executeUpdate(" DROP TABLE IF EXISTS SpJ ;");
			sql = "CREATE TABLE  `SpJ` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `SptimeIndex` ON `SpJ` (`TIMESTAMP` ASC);"; 
			System.out.println(sql);
			stmt.executeUpdate(sql);
			
			stmt.executeUpdate(" DROP TABLE IF EXISTS PGNR ;");
			sql = "CREATE TABLE  `PGNR` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `PGNRtimeIndex` ON `PGNR` (`TIMESTAMP` ASC);"; 
			System.out.println(sql);
			stmt.executeUpdate(sql);
			stmt.executeUpdate(" DROP TABLE IF EXISTS SSpJ ;");
			sql = "CREATE TABLE  `SSpJ` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `SsptimeIndex` ON `SSpJ` (`TIMESTAMP` ASC);"; 
			System.out.println(sql);
			stmt.executeUpdate(sql);
			stmt.executeUpdate(" DROP TABLE IF EXISTS FJ ;");
			sql = "CREATE TABLE  `FJ` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `FtimeIndex` ON `FJ` (`TIMESTAMP` ASC);"; 
			System.out.println(sql);
			stmt.executeUpdate(sql);
			stmt.executeUpdate(" DROP TABLE IF EXISTS ScJ ;");
			sql = "CREATE TABLE  `ScJ` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `SctimeIndex` ON `ScJ` (`TIMESTAMP` ASC);"; 
			System.out.println(sql);
			stmt.executeUpdate(sql);
			
			InputStream    fis;
			BufferedReader br;

			fis = new FileInputStream(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/SlidingOETJoinOperatorSSOutput.txt");
			br = new BufferedReader(new InputStreamReader(fis));
			String line=null;
			while((line=br.readLine())!=null)
			{
				String[] userInfo = line.split(" ");	
				sql = "INSERT INTO SSJ (USERID,MENTIONCOUNT,FOLLOWERCOUNT,TIMESTAMP) " +
						"VALUES ("+userInfo[0]+","+userInfo[1]+","+userInfo[2]+","+userInfo[3]+")"; 
				//System.out.println(sql);
				stmt.executeUpdate(sql);
			}


			//---------------------------------------------------------------------fill baseline table
			
			fis = new FileInputStream(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/PrefectSlidingOETSSOutput.txt");
			br = new BufferedReader(new InputStreamReader(fis));
			line=null;
			while((line=br.readLine())!=null)
			{
				String[] userInfo = line.split(" ");	
				sql = "INSERT INTO SSpJ (USERID,MENTIONCOUNT,FOLLOWERCOUNT,TIMESTAMP) " +
						"VALUES ("+userInfo[0]+","+userInfo[1]+","+userInfo[2]+","+userInfo[3]+")"; 
				//System.out.println(sql);
				stmt.executeUpdate(sql);
			}
//---------------------------------------------------------------------fill baseline table
			
			fis = new FileInputStream(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/WSJUpperBoundOutput.txt");
			br = new BufferedReader(new InputStreamReader(fis));
			line=null;
			while((line=br.readLine())!=null)
			{
				String[] userInfo = line.split(" ");	
				sql = "INSERT INTO PWSJ (USERID,MENTIONCOUNT,FOLLOWERCOUNT,TIMESTAMP) " +
						"VALUES ("+userInfo[0]+","+userInfo[1]+","+userInfo[2]+","+userInfo[3]+")"; 
				//System.out.println(sql);
				stmt.executeUpdate(sql);
			}
//---------------------------------------------------------------------fill baseline table
			
			fis = new FileInputStream(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/LRUWithOutWindowsLocalityOutput.txt");
			br = new BufferedReader(new InputStreamReader(fis));
			line=null;
			while((line=br.readLine())!=null)
			{
				String[] userInfo = line.split(" ");	
				//System.out.println(line);
				sql = "INSERT INTO LRUNLJ (USERID,MENTIONCOUNT,FOLLOWERCOUNT,TIMESTAMP) " +
						"VALUES ("+userInfo[0]+","+userInfo[1]+","+userInfo[2]+","+userInfo[3]+")"; 
				//System.out.println(sql);
				stmt.executeUpdate(sql);
			}
//---------------------------------------------------------------------fill baseline table
			
			fis = new FileInputStream(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/RandomWithOutWindowsLocalityOutput.txt");
			br = new BufferedReader(new InputStreamReader(fis));
			line=null;
			while((line=br.readLine())!=null)
			{
				String[] userInfo = line.split(" ");	
				sql = "INSERT INTO RNLJ (USERID,MENTIONCOUNT,FOLLOWERCOUNT,TIMESTAMP) " +
						"VALUES ("+userInfo[0]+","+userInfo[1]+","+userInfo[2]+","+userInfo[3]+")"; 
				//System.out.println(sql);
				stmt.executeUpdate(sql);
			}

			//---------------------------------------------------------------------fill baseline table
			fis = new FileInputStream(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/LRUJoinOperatorOutput.txt");
			br = new BufferedReader(new InputStreamReader(fis));
			line=null;
			while((line=br.readLine())!=null)
			{
				String[] userInfo = line.split(" ");	
				sql = "INSERT INTO LRUJ (USERID,MENTIONCOUNT,FOLLOWERCOUNT,TIMESTAMP) " +
						"VALUES ("+userInfo[0]+","+userInfo[1]+","+userInfo[2]+","+userInfo[3]+")"; 
				//System.out.println(sql);
				stmt.executeUpdate(sql);
			}
			//---------------------------------------------------------------------fill DWJ
			fis = new FileInputStream(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/DWJoinOperatorOutput.txt");
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
			fis = new FileInputStream(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/OracleJoinOperatorOutput.txt");
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
			fis = new FileInputStream(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/RandomCacheUpdateJoinOutput.txt");
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
			fis = new FileInputStream(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/SlidingOETJoinOperatorTSOutput.txt");
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
			//-----------------------------------------------------------------------fill classqueryProcessorOracleJoinOperatorOutput
			fis = new FileInputStream(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/GNRUpperBoundOutput.txt");
			br = new BufferedReader(new InputStreamReader(fis));
			line=null;
			while((line=br.readLine())!=null)
			{
				String[] userInfo = line.split(" ");	
				sql = "INSERT INTO PGNR (USERID,MENTIONCOUNT,FOLLOWERCOUNT,TIMESTAMP) " +
						"VALUES ("+userInfo[0]+","+userInfo[1]+","+userInfo[2]+","+userInfo[3]+")"; 
				//System.out.println(sql);
				stmt.executeUpdate(sql);
			}	
			
			//-----------------------------------------------------------------------fill classqueryProcessorOracleJoinOperatorOutput
			fis = new FileInputStream(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/PrefectSlidingOETTSOutput.txt");
			br = new BufferedReader(new InputStreamReader(fis));
			line=null;
			while((line=br.readLine())!=null)
			{
				String[] userInfo = line.split(" ");	
				sql = "INSERT INTO SpJ (USERID,MENTIONCOUNT,FOLLOWERCOUNT,TIMESTAMP) " +
						"VALUES ("+userInfo[0]+","+userInfo[1]+","+userInfo[2]+","+userInfo[3]+")"; 
				//System.out.println(sql);
				stmt.executeUpdate(sql);
			}	
			
			//---------------------------------------------------------------------fill FJ
			fis = new FileInputStream(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/FilteringJoinOperatorOutput.txt");
			br = new BufferedReader(new InputStreamReader(fis));
			line=null;
			while((line=br.readLine())!=null)
			{
				String[] userInfo = line.split(" ");	
				sql = "INSERT INTO FJ (USERID,MENTIONCOUNT,FOLLOWERCOUNT,TIMESTAMP) " +
						"VALUES ("+userInfo[0]+","+userInfo[1]+","+userInfo[2]+","+userInfo[3]+")"; 
				//System.out.println(sql);
				stmt.executeUpdate(sql);
			}
			
			//---------------------------------------------------------------------fill ScJ
			fis = new FileInputStream(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/ScoringJoinOperatorOutput.txt");
			br = new BufferedReader(new InputStreamReader(fis));
			line=null;
			while((line=br.readLine())!=null)
			{
				String[] userInfo = line.split(" ");	
				sql = "INSERT INTO ScJ (USERID,MENTIONCOUNT,FOLLOWERCOUNT,TIMESTAMP) " +
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

	public static TreeMap<Long,Integer> computeOJoin(){
		TreeMap<Long,Integer> result=new TreeMap<Long, Integer>();
		Connection c = null;
		Statement stmt = null;
		try {		
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());	      
			c.setAutoCommit(false);
			stmt = c.createStatement();
			String sql="SELECT oj.TIMESTAMP as TIMESTAMP, count(OJ.USERID) as windowcount FROM OJ  group by OJ.TIMESTAMP order by OJ.TIMESTAMP ASC";      //System.out.println(sql);
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
	public static HashMap<Long,Integer> computeLRUJoinPrecision(){
		HashMap<Long,Integer> result=new HashMap<Long, Integer>();
		Connection c = null;
		Statement stmt = null;
		try {		
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());	      
			c.setAutoCommit(false);
			stmt = c.createStatement();
			String sql="SELECT LRUJ.TIMESTAMP as TIMESTAMP , COUNT(*) as ERROR FROM LRUJ,OJ  WHERE LRUJ.USERID=OJ.USERID AND LRUJ.TIMESTAMP=OJ.TIMESTAMP AND LRUJ.FOLLOWERCOUNT <> OJ.FOLLOWERCOUNT group by OJ.TIMESTAMP";
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
	public static HashMap<Long,Integer> computeLRUNLJoinPrecision(){
		HashMap<Long,Integer> result=new HashMap<Long, Integer>();
		Connection c = null;
		Statement stmt = null;
		try {		
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());	      
			c.setAutoCommit(false);
			stmt = c.createStatement();
			String sql="SELECT LRUNLJ.TIMESTAMP as TIMESTAMP , COUNT(*) as ERROR FROM LRUNLJ,OJ  WHERE LRUNLJ.USERID=OJ.USERID AND LRUNLJ.TIMESTAMP=OJ.TIMESTAMP AND LRUNLJ.FOLLOWERCOUNT <> OJ.FOLLOWERCOUNT group by OJ.TIMESTAMP";
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
	public static HashMap<Long,Integer> computePWSJoinPrecision(){
		HashMap<Long,Integer> result=new HashMap<Long, Integer>();
		Connection c = null;
		Statement stmt = null;
		try {		
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());	      
			c.setAutoCommit(false);
			stmt = c.createStatement();
			String sql="SELECT PWSJ.TIMESTAMP as TIMESTAMP , COUNT(*) as ERROR FROM PWSJ,OJ  WHERE PWSJ.USERID=OJ.USERID AND PWSJ.TIMESTAMP=OJ.TIMESTAMP AND PWSJ.FOLLOWERCOUNT <> OJ.FOLLOWERCOUNT group by OJ.TIMESTAMP";
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
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());	      
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
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());	      
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
	public static HashMap<Long,Integer> computeRNLJoinPrecision(){
		HashMap<Long,Integer> result=new HashMap<Long, Integer>();
		Connection c = null;
		Statement stmt = null;
		try {		
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());	      
			c.setAutoCommit(false);
			stmt = c.createStatement();
			//String sql="SELECT DWJ.TIMESTAMP AS TIME, SUM(ABS(DWJ.FOLLOWERCOUNT-OJ.FOLLOWERCOUNT)) AS ERROR "+
			//		  " FROM DWJ,OJ  WHERE DWJ.USERID=OJ.USERID AND DWJ.TIMESTAMP=OJ.TIMESTAMP group by OJ.TIMESTAMP";
			String sql="SELECT RNLJ.TIMESTAMP as TIMESTAMP , COUNT(*) as ERROR FROM RNLJ,OJ  WHERE RNLJ.USERID=OJ.USERID AND RNLJ.TIMESTAMP=OJ.TIMESTAMP AND RNLJ.FOLLOWERCOUNT <> OJ.FOLLOWERCOUNT group by OJ.TIMESTAMP";

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
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());	      
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
	public static HashMap<Long,Integer> computeSslidingJoinPrecision(){
		HashMap<Long,Integer> result=new HashMap<Long, Integer>();
		Connection c = null;
		Statement stmt = null;
		try {		
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());	      
			c.setAutoCommit(false);
			stmt = c.createStatement();
			//String sql="SELECT DWJ.TIMESTAMP AS TIME, SUM(ABS(DWJ.FOLLOWERCOUNT-OJ.FOLLOWERCOUNT)) AS ERROR "+
			//		  " FROM DWJ,OJ  WHERE DWJ.USERID=OJ.USERID AND DWJ.TIMESTAMP=OJ.TIMESTAMP group by OJ.TIMESTAMP";
			String sql="SELECT SSJ.TIMESTAMP as TIMESTAMP , COUNT(*) as ERROR FROM SSJ,OJ  WHERE SSJ.USERID=OJ.USERID AND SSJ.TIMESTAMP=OJ.TIMESTAMP AND SSJ.FOLLOWERCOUNT <> OJ.FOLLOWERCOUNT group by OJ.TIMESTAMP";

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
	public static HashMap<Long,Integer> computePrefectSJoinPrecision(){
		HashMap<Long,Integer> result=new HashMap<Long, Integer>();
		Connection c = null;
		Statement stmt = null;
		try {		
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());	      
			c.setAutoCommit(false);
			stmt = c.createStatement();
			//String sql="SELECT DWJ.TIMESTAMP AS TIME, SUM(ABS(DWJ.FOLLOWERCOUNT-OJ.FOLLOWERCOUNT)) AS ERROR "+
			//		  " FROM DWJ,OJ  WHERE DWJ.USERID=OJ.USERID AND DWJ.TIMESTAMP=OJ.TIMESTAMP group by OJ.TIMESTAMP";
			String sql="SELECT SpJ.TIMESTAMP as TIMESTAMP , COUNT(*) as ERROR FROM SpJ,OJ  WHERE SpJ.USERID=OJ.USERID AND SpJ.TIMESTAMP=OJ.TIMESTAMP AND SpJ.FOLLOWERCOUNT <> OJ.FOLLOWERCOUNT group by OJ.TIMESTAMP";

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
	public static HashMap<Long,Integer> computePrefectSslidingJoinPrecision(){
		HashMap<Long,Integer> result=new HashMap<Long, Integer>();
		Connection c = null;
		Statement stmt = null;
		try {		
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());	      
			c.setAutoCommit(false);
			stmt = c.createStatement();
			//String sql="SELECT DWJ.TIMESTAMP AS TIME, SUM(ABS(DWJ.FOLLOWERCOUNT-OJ.FOLLOWERCOUNT)) AS ERROR "+
			//		  " FROM DWJ,OJ  WHERE DWJ.USERID=OJ.USERID AND DWJ.TIMESTAMP=OJ.TIMESTAMP group by OJ.TIMESTAMP";
			String sql="SELECT SSpJ.TIMESTAMP as TIMESTAMP , COUNT(*) as ERROR FROM SSpJ,OJ  WHERE SSpJ.USERID=OJ.USERID AND SSpJ.TIMESTAMP=OJ.TIMESTAMP AND SSpJ.FOLLOWERCOUNT <> OJ.FOLLOWERCOUNT group by OJ.TIMESTAMP";

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
	public static HashMap<Long,Integer> computePGNRJoinPrecision(){
		HashMap<Long,Integer> result=new HashMap<Long, Integer>();
		Connection c = null;
		Statement stmt = null;
		try {		
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());	      
			c.setAutoCommit(false);
			stmt = c.createStatement();
			//String sql="SELECT DWJ.TIMESTAMP AS TIME, SUM(ABS(DWJ.FOLLOWERCOUNT-OJ.FOLLOWERCOUNT)) AS ERROR "+
			//		  " FROM DWJ,OJ  WHERE DWJ.USERID=OJ.USERID AND DWJ.TIMESTAMP=OJ.TIMESTAMP group by OJ.TIMESTAMP";
			String sql="SELECT PGNR.TIMESTAMP as TIMESTAMP , COUNT(*) as ERROR FROM PGNR,OJ  WHERE PGNR.USERID=OJ.USERID AND PGNR.TIMESTAMP=OJ.TIMESTAMP AND PGNR.FOLLOWERCOUNT <> OJ.FOLLOWERCOUNT group by OJ.TIMESTAMP";

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
	public static HashMap<Long,Integer> computeFJoinPrecision(){
		HashMap<Long,Integer> result=new HashMap<Long, Integer>();
		Connection c = null;
		Statement stmt = null;
		try {		
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());	      
			c.setAutoCommit(false);
			stmt = c.createStatement();
			//String sql="SELECT DWJ.TIMESTAMP AS TIME, SUM(ABS(DWJ.FOLLOWERCOUNT-OJ.FOLLOWERCOUNT)) AS ERROR "+
			//		  " FROM DWJ,OJ  WHERE DWJ.USERID=OJ.USERID AND DWJ.TIMESTAMP=OJ.TIMESTAMP group by OJ.TIMESTAMP";
			String sql="SELECT FJ.TIMESTAMP as TIMESTAMP , COUNT(*) as ERROR FROM FJ,OJ  WHERE FJ.USERID=OJ.USERID AND FJ.TIMESTAMP=OJ.TIMESTAMP AND FJ.FOLLOWERCOUNT <> OJ.FOLLOWERCOUNT group by OJ.TIMESTAMP";

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
	public static HashMap<Long,Integer> computeScJoinPrecision(){
		HashMap<Long,Integer> result=new HashMap<Long, Integer>();
		Connection c = null;
		Statement stmt = null;
		try {		
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());	      
			c.setAutoCommit(false);
			stmt = c.createStatement();
			//String sql="SELECT DWJ.TIMESTAMP AS TIME, SUM(ABS(DWJ.FOLLOWERCOUNT-OJ.FOLLOWERCOUNT)) AS ERROR "+
			//		  " FROM DWJ,OJ  WHERE DWJ.USERID=OJ.USERID AND DWJ.TIMESTAMP=OJ.TIMESTAMP group by OJ.TIMESTAMP";
			String sql="SELECT ScJ.TIMESTAMP as TIMESTAMP , COUNT(*) as ERROR FROM ScJ,OJ  WHERE ScJ.USERID=OJ.USERID AND ScJ.TIMESTAMP=OJ.TIMESTAMP AND ScJ.FOLLOWERCOUNT <> OJ.FOLLOWERCOUNT group by OJ.TIMESTAMP";

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
			insertResultToDB();
			BufferedWriter bw=new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare.csv")));
			TreeMap<Long,Integer> oracleCount=computeOJoin();
			HashMap<Long,Integer> SError=computeSJoinPrecision();
			HashMap<Long,Integer> SSError=computeSslidingJoinPrecision();
			HashMap<Long,Integer> RError=computeRJoinPrecision();
			HashMap<Long,Integer> DWError=computeDWJoinPrecision();
			HashMap<Long,Integer> BError=computeLRUJoinPrecision();
			HashMap<Long,Integer> SpError=computePrefectSJoinPrecision();
			HashMap<Long,Integer> SSpError=computePrefectSslidingJoinPrecision();
			HashMap<Long,Integer> LRUNLError=computeLRUNLJoinPrecision();
			HashMap<Long,Integer> RNLError=computeRNLJoinPrecision();
			HashMap<Long,Integer> PGNRError=computePGNRJoinPrecision();
			HashMap<Long,Integer> PWSError=computePWSJoinPrecision();
			HashMap<Long,Integer> FError=computeFJoinPrecision();
			HashMap<Long,Integer> ScError=computeScJoinPrecision();
			
			Iterator<Long> itO = oracleCount.keySet().iterator();

			bw.write("timestampe,oracle,DW,Smart,random,LRU,slidingSmart,PrefectSmart, PrefectSlidingSmart,LRUNL,RNL,PGNR,PWSJ,Filter,score,");
			bw.write("cumulative oracle,cumulative DW,cumulative Smart,cumulative random,cumulative LRU,cumulative slidingSmart,cumulative PrefectSmart,cumulative PrefectSlidingSmart,cumulative LRUNL,cumulative RNL,cumulative PGNR,cumulative PWSJ,cumulative Filter,cumulative score\n");

			
			
			Integer cOC=0, cdwe=0, cse=0, cre=0, cbe=0, csse=0, cspe=0, csspe=0, clrunle=0, crnle=0, cgnre=0, cpwse=0, cfe=0, csce=0;
			
			while(itO.hasNext()){
				long nextTime = itO.next();
				
				
				Integer OC=oracleCount.get(nextTime);
				Integer dwe=DWError.get(nextTime);
				Integer se=SError.get(nextTime);
				Integer re=RError.get(nextTime);
				Integer be=BError.get(nextTime);
				Integer sse=SSError.get(nextTime);
				Integer spe=SpError.get(nextTime);
				Integer sspe=SSpError.get(nextTime);
				Integer lrunle=LRUNLError.get(nextTime);
				Integer rnle=RNLError.get(nextTime);
				Integer gnre=PGNRError.get(nextTime);
				Integer pwse=PWSError.get(nextTime);
				Integer fe=FError.get(nextTime);
				Integer sce=ScError.get(nextTime);
				
				

				//cumulative error
				cOC=cOC + OC ; 
				cdwe= cdwe + (dwe = dwe==null?0:dwe) ;
				cse= cse + (se= se==null?0:se) ;
				cre= cre + (re = re==null?0:re) ;
				cbe= cbe + (be = be==null?0:be); 
				csse= csse + (sse= sse==null?0:sse) ;
				cspe= cspe + (spe= spe==null?0:spe) ; 
				csspe= csspe + (sspe= sspe==null?0:sspe) ;
				clrunle= clrunle + ( lrunle= lrunle==null?0:lrunle) ;
				crnle= crnle + (rnle= rnle==null?0:rnle) ;
				cgnre= cgnre + (gnre= gnre==null?0:gnre) ;
				cpwse= cpwse + (pwse= pwse==null?0:pwse) ;
				cfe= cfe + (fe= fe==null?0:fe) ;
				csce= csce + (sce= sce==null?0:sce) ;
				
				bw.write(nextTime+","+OC+","+(dwe==null?0:dwe)+","+(se==null?0:se)+","+(re==null?0:re)+","+(be==null?0:be)+","+(sse==null?0:sse)+","+(spe==null?0:spe)+","+(sspe==null?0:sspe)+","+(lrunle==null?0:lrunle)+","+(rnle==null?0:rnle)+","+(gnre==null?0:gnre)+","+(pwse==null?0:pwse)+","+(fe==null?0:fe)+","+(sce==null?0:sce)+",");
				bw.write(cOC+","+(cdwe==null?0:cdwe)+","+(cse==null?0:cse)+","+(cre==null?0:cre)+","+(cbe==null?0:cbe)+","+(csse==null?0:csse)+","+(cspe==null?0:cspe)+","+(csspe==null?0:csspe)+","+(clrunle==null?0:clrunle)+","+(crnle==null?0:crnle)+","+(cgnre==null?0:cgnre)+","+(cpwse==null?0:cpwse)+","+(cfe==null?0:cfe)+","+(csce==null?0:csce)+" \n");


			}
			bw.flush();
			bw.close();
		}catch(Exception e){e.printStackTrace();}
	}
}
