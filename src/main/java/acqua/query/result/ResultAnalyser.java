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
			stmt.executeUpdate(" DROP TABLE IF EXISTS SSpJ ;");
			sql = "CREATE TABLE  `SSpJ` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `SsptimeIndex` ON `SSpJ` (`TIMESTAMP` ASC);"; 
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
			
			Iterator<Long> itO = oracleCount.keySet().iterator();

			bw.write("timestampe,oracle,DW,Smart,random,LRU,slidingSmart,PrefectSmart, PrefectSlidingSmart\n");
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
				
				bw.write(nextTime+","+OC+","+(dwe==null?0:dwe)+","+(se==null?0:se)+","+(re==null?0:re)+","+(be==null?0:be)+","+(sse==null?0:sse)+","+(spe==null?0:spe)+","+(sspe==null?0:sspe)+" \n");
			}
			bw.flush();
			bw.close();
		}catch(Exception e){e.printStackTrace();}
	}
}
