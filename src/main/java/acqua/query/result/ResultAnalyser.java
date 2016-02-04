package acqua.query.result;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Set;
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
			System.out.println("create tables");

			stmt = c.createStatement();
			//stmt.executeQuery("DROP INDEX IF EXISTS timeIndex ON BKG;");
			stmt.executeUpdate(" DROP TABLE IF EXISTS LRUJ ;");
			String sql = "CREATE TABLE  `LRUJ` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `LRUtimeIndex` ON `LRUJ` (`TIMESTAMP` ASC);"; 
			//System.out.println(sql);
			stmt.executeUpdate(sql);
			stmt.executeUpdate(" DROP TABLE IF EXISTS OJ ;");
			sql = "CREATE TABLE  `OJ` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `OtimeIndex` ON `OJ` (`TIMESTAMP` ASC);"; 
			//System.out.println(sql);
			stmt.executeUpdate(sql);
			stmt.executeUpdate(" DROP TABLE IF EXISTS LRUNLJ ;");
			sql = "CREATE TABLE  `LRUNLJ` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `LRUNLJtimeIndex` ON `LRUNLJ` (`TIMESTAMP` ASC);"; 
			//System.out.println(sql);
			stmt.executeUpdate(sql);
			stmt.executeUpdate(" DROP TABLE IF EXISTS RNLJ ;");
			sql = "CREATE TABLE  `RNLJ` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `RNLJtimeIndex` ON `RNLJ` (`TIMESTAMP` ASC);"; 
			//System.out.println(sql);
			stmt.executeUpdate(sql);
			
			stmt.executeUpdate(" DROP TABLE IF EXISTS DWJ ;");
			sql = "CREATE TABLE  `DWJ` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `DWtimeIndex` ON `DWJ` (`TIMESTAMP` ASC);"; 
			//System.out.println(sql);
			stmt.executeUpdate(sql);
			stmt.executeUpdate(" DROP TABLE IF EXISTS RJ ;");
			sql = "CREATE TABLE  `RJ` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `RtimeIndex` ON `RJ` (`TIMESTAMP` ASC);"; 
			//System.out.println(sql);
			stmt.executeUpdate(sql);
			stmt.executeUpdate(" DROP TABLE IF EXISTS SJ ;");
			sql = "CREATE TABLE  `SJ` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `StimeIndex` ON `SJ` (`TIMESTAMP` ASC);"; 
			//System.out.println(sql);
			stmt.executeUpdate(sql);
			stmt.executeUpdate(" DROP TABLE IF EXISTS PWSJ ;");
			sql = "CREATE TABLE  `PWSJ` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `PWSJtimeIndex` ON `PWSJ` (`TIMESTAMP` ASC);"; 
			//System.out.println(sql);
			stmt.executeUpdate(sql);
			stmt.executeUpdate(" DROP TABLE IF EXISTS SSJ ;");
			sql = "CREATE TABLE  `SSJ` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `SstimeIndex` ON `SSJ` (`TIMESTAMP` ASC);"; 
			//System.out.println(sql);
			stmt.executeUpdate(sql);

			stmt.executeUpdate(" DROP TABLE IF EXISTS SpJ ;");
			sql = "CREATE TABLE  `SpJ` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `SptimeIndex` ON `SpJ` (`TIMESTAMP` ASC);"; 
			//System.out.println(sql);
			stmt.executeUpdate(sql);
			
			stmt.executeUpdate(" DROP TABLE IF EXISTS PGNR ;");
			sql = "CREATE TABLE  `PGNR` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `PGNRtimeIndex` ON `PGNR` (`TIMESTAMP` ASC);"; 
			//System.out.println(sql);
			stmt.executeUpdate(sql);
			stmt.executeUpdate(" DROP TABLE IF EXISTS SSpJ ;");
			sql = "CREATE TABLE  `SSpJ` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `SsptimeIndex` ON `SSpJ` (`TIMESTAMP` ASC);"; 
			//System.out.println(sql);
			stmt.executeUpdate(sql);
			stmt.executeUpdate(" DROP TABLE IF EXISTS FJ ;");
			sql = "CREATE TABLE  `FJ` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `FtimeIndex` ON `FJ` (`TIMESTAMP` ASC);"; 
			//System.out.println(sql);
			stmt.executeUpdate(sql);
			stmt.executeUpdate(" DROP TABLE IF EXISTS ScJ ;");
			sql = "CREATE TABLE  `ScJ` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `SctimeIndex` ON `ScJ` (`TIMESTAMP` ASC);"; 
			//System.out.println(sql);
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
	public static void main(String[] args){
		
		String srcDB = Config.INSTANCE.getDatasetDb();
		String desDB = srcDB.split("\\.")[0]+"_2.db" ;
		Config.INSTANCE.setDatasetDb(desDB);
		//ResultAnalyser.analysisExperimentJaccard();
		
//		System.out.println ("  R join"); 
//		HashMap<Long,Integer> RError=computeErrors (getOracleUsersOfTimestaps() , getRJoinUsersOfTimestaps() );
//		System.out.println ("  DW join ");
//		HashMap<Long,Integer> DWError=computeErrors (getOracleUsersOfTimestaps() , getDWJoinUsersOfTimestaps() );
		HashMap<Long,Double> SpError= computeErrorsJaccardIndex  (getOracleUsersOfTimestaps() , getPrefectSJoinUsersOfTimestaps() );

	}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//Functions related to Top-k Aqua
	
	
	public static void analysisExperiment(){
		
		try{
			insertResultToDB();
			BufferedWriter bw=new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare.csv")));
			
			TreeMap<Long,Integer> oracleCount=computeOJoin();
			//System.out.println(" OracleCount size  = " + oracleCount.size());

			HashMap<Long,Integer> SSError=computeErrors (getOracleUsersOfTimestaps() , getSslidingJoinUsersOfTimestaps() );
			HashMap<Long,Integer> RError=computeErrors (getOracleUsersOfTimestaps() , getRJoinUsersOfTimestaps() );
			HashMap<Long,Integer> DWError=computeErrors (getOracleUsersOfTimestaps() , getDWJoinUsersOfTimestaps() );
			HashMap<Long,Integer> LRUError=computeErrors (getOracleUsersOfTimestaps() , getLRUJoinUsersOfTimestaps() );
			HashMap<Long,Integer> SpError=computeErrors (getOracleUsersOfTimestaps() , getPrefectSJoinUsersOfTimestaps() );
			HashMap<Long,Integer> FError=computeErrors (getOracleUsersOfTimestaps() , getFJoinUsersOfTimestaps() );
			
			
			Iterator<Long> itO = oracleCount.keySet().iterator();
			bw.write("timestampe,Oracle,WST,WSJ-RND,WSJ-WBM,WSJ-LRU,WSJ-WBM*,Filter\n");

			Integer cOC=0, cdwe=0, cre=0,csse=0, clrue=0, cfe=0, cspe=0 ;
			
			while(itO.hasNext()){
				
				long nextTime = itO.next();
				Integer OC=oracleCount.get(nextTime);
				Integer dwe=DWError.get(nextTime);
				Integer re=RError.get(nextTime);
				Integer sse=SSError.get(nextTime);
				Integer lrue=LRUError.get(nextTime);
				Integer spe=SpError.get(nextTime);
				Integer fe=FError.get(nextTime);

				//cumulative error
				cOC=cOC + OC ; 
				cdwe= cdwe + (dwe = dwe==null?0:dwe) ;
				cre= cre + (re = re==null?0:re) ;
				csse= csse + (sse= sse==null?0:sse) ;
				clrue= clrue + (lrue= lrue==null?0:lrue) ;
				cspe= cspe + (spe= spe==null?0:spe) ;
				cfe= cfe + (fe= fe==null?0:fe) ;
				
				bw.write(nextTime+","+cOC+","+(cdwe==null?0:cdwe)+","+(cre==null?0:cre)+","+(csse==null?0:csse)+"," +(clrue==null?0:clrue)+","+(cspe==null?0:cspe)+"," +(cfe==null?0:cfe)+"\n");

			}
			bw.flush();
			bw.close();
		}catch(Exception e){e.printStackTrace();}
		
	}

	public static void analysisExperimentJaccard(){
		
		try{
			insertResultToDB();
			BufferedWriter bw=new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare.csv")));
			
			TreeMap<Long,Integer> oracleCount=computeOJoin();
			//System.out.println(" OracleCount size  = " + oracleCount.size());


			//use Jaccard Index for computing errors
			HashMap<Long,Double> SSError=computeErrorsJaccardIndex(getOracleUsersOfTimestaps() , getSslidingJoinUsersOfTimestaps() );
			HashMap<Long,Double> RError=computeErrorsJaccardIndex (getOracleUsersOfTimestaps() , getRJoinUsersOfTimestaps() );
			HashMap<Long,Double> DWError=computeErrorsJaccardIndex (getOracleUsersOfTimestaps() , getDWJoinUsersOfTimestaps() );
			HashMap<Long,Double> LRUError=computeErrorsJaccardIndex (getOracleUsersOfTimestaps() , getLRUJoinUsersOfTimestaps() );
			HashMap<Long,Double> SpError=computeErrorsJaccardIndex (getOracleUsersOfTimestaps() , getPrefectSJoinUsersOfTimestaps() );
			HashMap<Long,Double> FError=computeErrorsJaccardIndex(getOracleUsersOfTimestaps() , getFJoinUsersOfTimestaps() );
			
			
			Iterator<Long> itO = oracleCount.keySet().iterator();
			bw.write("timestampe,Oracle,WST,WSJ-RND,WSJ-WBM,WSJ-LRU,WSJ-WBM*,Filter\n");

			Double cOC=0.0, cdwe=0.0, cre=0.0,csse=0.0, clrue=0.0, cfe=0.0, cspe=0.0 ;
			
			while(itO.hasNext()){
				
				long nextTime = itO.next();
				Integer OC=oracleCount.get(nextTime);
				Double dwe=DWError.get(nextTime);
				Double re=RError.get(nextTime);
				Double sse=SSError.get(nextTime);
				Double lrue=LRUError.get(nextTime);
				Double spe=SpError.get(nextTime);
				Double fe=FError.get(nextTime);
				//System.out.println ("time = " + nextTime + " oc = " + OC + "  dwe = " + dwe + "   re  = " + re + "spe = " + spe + "  fe = " + fe);

				//cumulative error
				cOC=cOC + OC ; 
				cdwe= cdwe + (dwe = dwe==null?0:dwe) ;
				cre= cre + (re = re==null?0:re) ;
				csse= csse + (sse= sse==null?0:sse) ;
				clrue= clrue + (lrue= lrue==null?0:lrue) ;
				cspe= cspe + (spe= spe==null?0:spe) ;
				cfe= cfe + (fe= fe==null?0:fe) ;
				
			//	bw.write(nextTime+","+cOC+","+   (cdwe==null?0:cdwe)+","+(cre==null?0:cre)+","+(csse==null?0:csse)+"," +(clrue==null?0:clrue)+","+(cspe==null?0:cspe)+"," +(cfe==null?0:cfe)+"\n");
				
				
				bw.write(nextTime+","+String.format("%.2f",cOC)+
						","+ String.format("%.2f",(cdwe==null?0:cdwe))+ 
						","+ String.format("%.2f",(cre==null?0:cre))+
						","+ String.format("%.2f",(csse==null?0:csse)) +
						","+ String.format("%.2f",(clrue==null?0:clrue)) +
						","+ String.format("%.2f",(cspe==null?0:cspe))+
						","+ String.format("%.2f",(cfe==null?0:cfe))+"\n");

				
			}
			bw.flush();
			bw.close();
		}catch(Exception e){e.printStackTrace();}
		
	}

	public static void analysisMultipleExperiments(){
		
		try{
			
			BufferedWriter bw=new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compareMultipleExperiments.csv")));
			//bw.write("timestampe,cumulative oracle Min,cumulative DW Min,cumulative random Min,cumulative slidingSmart Min,cumulative LRUJ Min,cumulative perfect smart Min,cumulative Filter Min,cumulative oracle Max,cumulative DW Max,cumulative random Max,cumulative slidingSmart Max,cumulative LRUJ Max,cumulative perfect smart Max,cumulative Filter Max, cumulative oracle Avg,cumulative DW Avg,cumulative random Avg,cumulative slidingSmart Avg,cumulative LRUJ Avg,cumulative perfect smart Avg,cumulative Filter Avg\n");
			bw.write("timestampe,Oracle Min,WST Min,WSJ-RND Min,WSJ-WBM Min,WSJ-LRU Min,WSJ-WBM* Min,Filter Min,Oracle Max,WST Min,WSJ-RND Max,WSJ-WBM Max,WSJ-LRU Max,WSJ-WBM* Max,Filter Max,Oracle Avg,WST Avg,WSJ-RND Avg,WSJ-WBM Avg,WSJ-LRU Avg,WSJ-WBM* Avg,Filter Avg\n");
			
			
			//Min variables
			Integer OCMin = Integer.MAX_VALUE, dweMin = Integer.MAX_VALUE, reMin = Integer.MAX_VALUE, sseMin = Integer.MAX_VALUE, lrueMin = Integer.MAX_VALUE, speMin = Integer.MAX_VALUE, feMin = Integer.MAX_VALUE;
			//Max variables
			Integer OCMax = 0, dweMax= 0, reMax = 0,sseMax = 0, lrueMax = 0, speMax = 0, feMax = 0;
			//Average variables 
			Integer OCAvg = 0, dweAvg = 0, reAvg = 0, sseAvg = 0, lrueAvg = 0, speAvg = 0 , feAvg = 0;
			//Sum variables 
			Integer OCSum = 0, dweSum = 0, reSum = 0, sseSum = 0, lrueSum = 0, speSum = 0 , feSum = 0;
			long nextTime = 0;
			
			for ( int e = 0 ; e <= 90 ; e++){
				
			
			for ( int i = 1 ; i<= Config.INSTANCE.getDatabaseNumber() ; i++){
				
				BufferedReader br=new BufferedReader(new FileReader(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare_"+ i +".csv")));
				String line ="";
				
				for( int k =0;k<=e; k++){
					line = br.readLine();
				}
				if (e==0)
					break;
				//System.out.println("read second line" + e + " i = " + i  +"line = "+ line);
				String [] lineSplit = line.split(",| ");
				nextTime = Long.parseLong(lineSplit[0]);
				Integer OC = Integer.parseInt(lineSplit[1]);
				Integer dwe = Integer.parseInt(lineSplit[2]);
				Integer re = Integer.parseInt(lineSplit[3]);
				Integer sse = Integer.parseInt(lineSplit[4]);
				Integer lrue = Integer.parseInt(lineSplit[5]);
				Integer spe = Integer.parseInt(lineSplit[6]);
				Integer fe = Integer.parseInt(lineSplit[7]);
				
				
				if (OC < OCMin )  OCMin = OC;
				if (dwe < dweMin )  dweMin = dwe;
				if (re < reMin )  reMin = re;
				if (sse < sseMin )  sseMin = sse;
				if (lrue < lrueMin )  lrueMin = lrue;
				if (spe < speMin) speMin = spe;
				if (fe < feMin )  feMin = fe;
				
				if (OC > OCMax )  OCMax = OC;
				if (dwe > dweMax )  dweMax = dwe;
				if (re > reMax )  reMax = re;
				if (sse > sseMax )  sseMax = sse;
				if (lrue > lrueMax )  lrueMax = lrue;
				if (spe > speMax) speMax = spe;
				if (fe > feMax )  feMax = fe;

				OCSum += OC;
				dweSum += dwe;
				reSum += re;
				sseSum += sse;
				lrueSum += lrue;
				speSum +=spe;
				feSum += fe;
				
			}
			
			int totalNum = Config.INSTANCE.getDatabaseNumber();
			OCAvg = OCSum / totalNum;
			dweAvg = dweSum / totalNum;
			reAvg = reSum / totalNum;
			sseAvg = sseSum / totalNum;
			lrueAvg = lrueSum / totalNum;
			speAvg = speSum / totalNum;
			feAvg = feSum / totalNum;
			
			
			
			bw.write(nextTime+","+OCMin+","+(dweMin==null?0:dweMin)+","+(reMin==null?0:reMin)+","+(sseMin==null?0:sseMin)+"," +(lrueMin==null?0:lrueMin)+","+(speMin==null?0:speMin)+","+(feMin==null?0:feMin)
					+","+OCMax+","+(dweMax==null?0:dweMax)+","+(reMax==null?0:reMax)+","+(sseMax==null?0:sseMax)+"," +(lrueMax==null?0:lrueMax)+","+(speMax==null?0:speMax)+","+(feMax==null?0:feMax)
					+","+OCAvg+","+(dweAvg)+","+(reAvg)+","+(sseAvg)+"," +(lrueAvg)+","+(speAvg)+","+(feAvg)+"\n");

			OCMin = Integer.MAX_VALUE; dweMin = Integer.MAX_VALUE; reMin = Integer.MAX_VALUE; sseMin = Integer.MAX_VALUE; lrueMin = Integer.MAX_VALUE; speMin =Integer.MAX_VALUE;  feMin = Integer.MAX_VALUE;
			OCMax = 0; dweMax= 0; reMax = 0; sseMax = 0; lrueMax = 0; speMax = 0 ;feMax = 0;
			OCAvg = 0; dweAvg = 0; reAvg = 0; sseAvg = 0; lrueAvg = 0;speAvg = 0; feAvg = 0;
			OCSum = 0; dweSum = 0; reSum = 0; sseSum = 0; lrueSum = 0; speSum = 0; feSum = 0;
			}
			bw.close();
		}catch(Exception e){e.printStackTrace();}
		
	}

	public static void analysisMultipleExperimentsJaccard(int index ){
		
		try{
			
			BufferedWriter bw=new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compareMultipleExperiments.csv")));
			//bw.write("timestampe,cumulative oracle Min,cumulative DW Min,cumulative random Min,cumulative slidingSmart Min,cumulative LRUJ Min,cumulative perfect smart Min,cumulative Filter Min,cumulative oracle Max,cumulative DW Max,cumulative random Max,cumulative slidingSmart Max,cumulative LRUJ Max,cumulative perfect smart Max,cumulative Filter Max, cumulative oracle Avg,cumulative DW Avg,cumulative random Avg,cumulative slidingSmart Avg,cumulative LRUJ Avg,cumulative perfect smart Avg,cumulative Filter Avg\n");
			bw.write("timestampe,Oracle Min,WST Min,WSJ-RND Min,WSJ-WBM Min,WSJ-LRU Min,WSJ-WBM* Min,Filter Min,Oracle Max,WST Min,WSJ-RND Max,WSJ-WBM Max,WSJ-LRU Max,WSJ-WBM* Max,Filter Max,Oracle Avg,WST Avg,WSJ-RND Avg,WSJ-WBM Avg,WSJ-LRU Avg,WSJ-WBM* Avg,Filter Avg\n");
			
			
			//Min variables
			Double OCMin = Double.MAX_VALUE, dweMin = Double.MAX_VALUE, reMin = Double.MAX_VALUE, sseMin = Double.MAX_VALUE, lrueMin = Double.MAX_VALUE, speMin = Double.MAX_VALUE, feMin = Double.MAX_VALUE;
			//Max variables
			Double OCMax = 0.0, dweMax= 0.0, reMax = 0.0,sseMax = 0.0, lrueMax = 0.0, speMax = 0.0, feMax = 0.0;
			//Average variables 
			Double OCAvg = 0.0, dweAvg = 0.0, reAvg = 0.0, sseAvg = 0.0, lrueAvg = 0.0, speAvg = 0.0 , feAvg = 0.0;
			//Sum variables 
			Double OCSum = 0.0, dweSum = 0.0, reSum = 0.0, sseSum = 0.0, lrueSum = 0.0, speSum = 0.0 , feSum = 0.0;
			long nextTime = 0;
			
			for ( int e = 0 ; e <= Config.INSTANCE.getExperimentIterationNumber() ; e++){
				
				int nullLine = 0;
				for ( int i = 1 ; i<= Config.INSTANCE.getDatabaseNumber() ; i++){
					
					BufferedReader br=new BufferedReader(new FileReader(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare_"+ i +"_" + index + ".csv")));
					String line ="";
					
					for( int k =0;k<=e; k++){
						line = br.readLine();
					}
					if (e==0)
						break;
					//System.out.println("read second line" + e + " i = " + i  +"line = "+ line);
					if (line == null){
						nullLine ++;
						System.out.println("iteration = " + e + "   database = " + i  +"   null line = "+ nullLine);
						continue ;
					}
					String [] lineSplit = line.split(",| ");
					nextTime = Long.parseLong(lineSplit[0]);
					Double OC = Double.parseDouble(lineSplit[1]);
					Double dwe = Double.parseDouble(lineSplit[2]);
					Double re = Double.parseDouble(lineSplit[3]);
					Double sse = Double.parseDouble(lineSplit[4]);
					Double lrue = Double.parseDouble(lineSplit[5]);
					Double spe = Double.parseDouble(lineSplit[6]);
					Double fe = Double.parseDouble(lineSplit[7]);
				
					if (OC < OCMin )  OCMin = OC;
					if (dwe < dweMin )  dweMin = dwe;
					if (re < reMin )  reMin = re;
					if (sse < sseMin )  sseMin = sse;
					if (lrue < lrueMin )  lrueMin = lrue;
					if (spe < speMin) speMin = spe;
					if (fe < feMin )  feMin = fe;
					
					if (OC > OCMax )  OCMax = OC;
					if (dwe > dweMax )  dweMax = dwe;
					if (re > reMax )  reMax = re;
					if (sse > sseMax )  sseMax = sse;
					if (lrue > lrueMax )  lrueMax = lrue;
					if (spe > speMax) speMax = spe;
					if (fe > feMax )  feMax = fe;
	
					OCSum += OC;
					dweSum += dwe;
					System.out.println("wst error =  " + dwe);
					reSum += re;
					sseSum += sse;
					lrueSum += lrue;
					speSum +=spe;
					feSum += fe;
				
				}
			
				int totalNum = Config.INSTANCE.getDatabaseNumber() - nullLine;
				
				OCAvg = OCSum / totalNum;
				dweAvg = dweSum / totalNum;
				reAvg = reSum / totalNum;
				sseAvg = sseSum / totalNum;
				lrueAvg = lrueSum / totalNum;
				speAvg = speSum / totalNum;
				feAvg = feSum / totalNum;
				System.out.println("total num =  " +totalNum + "   wst sum = " + dweSum + "    wst avg = " + dweAvg);
				
				bw.write(nextTime+","+OCMin+","+(dweMin==null?0:dweMin)+","+(reMin==null?0:reMin)+","+(sseMin==null?0:sseMin)+"," +(lrueMin==null?0:lrueMin)+","+(speMin==null?0:speMin)+","+(feMin==null?0:feMin)
						+","+OCMax+","+(dweMax==null?0:dweMax)+","+(reMax==null?0:reMax)+","+(sseMax==null?0:sseMax)+"," +(lrueMax==null?0:lrueMax)+","+(speMax==null?0:speMax)+","+(feMax==null?0:feMax)
						+","+OCAvg+","+(dweAvg)+","+(reAvg)+","+(sseAvg)+"," +(lrueAvg)+","+(speAvg)+","+(feAvg)+"\n");
	
				OCMin = Double.MAX_VALUE; dweMin = Double.MAX_VALUE; reMin = Double.MAX_VALUE; sseMin = Double.MAX_VALUE; lrueMin = Double.MAX_VALUE; speMin =Double.MAX_VALUE;  feMin = Double.MAX_VALUE;
				OCMax = 0.0; dweMax= 0.0; reMax = 0.0; sseMax = 0.0; lrueMax = 0.0; speMax = 0.0 ;feMax = 0.0;
				OCAvg = 0.0; dweAvg = 0.0; reAvg = 0.0; sseAvg = 0.0; lrueAvg = 0.0;speAvg = 0.0; feAvg = 0.0;
				OCSum = 0.0; dweSum = 0.0; reSum = 0.0; sseSum = 0.0; lrueSum = 0.0; speSum = 0.0; feSum = 0.0;
			}
			bw.close();
		}catch(Exception e){e.printStackTrace();}
		
	}

	public static TreeMap< Long, HashSet<Integer>> getOracleUsersOfTimestaps(){
		
		TreeMap< Long, HashSet<Integer>> result = new TreeMap< Long, HashSet<Integer>>();
		Connection c = null;
		Statement stmt = null, stmt1 = null;
		try {		
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());	      
			c.setAutoCommit(false);
			stmt = c.createStatement();
			stmt1 = c.createStatement();
			
			String sql="SELECT DISTINCT OJ.TIMESTAMP FROM OJ ";
			ResultSet rs = stmt.executeQuery( sql);

			while ( rs.next() ) {
				Long timeStamp  = rs.getLong("TIMESTAMP");
				String sql1="SELECT OJ.USERID FROM OJ WHERE OJ.TIMESTAMP = " + timeStamp ;
				ResultSet rs1 = stmt1.executeQuery( sql1);
				HashSet<Integer> userSet = new HashSet<Integer>() ;
				
				while ( rs1.next() ) {
					Integer userID  = rs1.getInt("USERID");
					userSet.add(userID);
				}
				result.put(timeStamp,userSet);
				rs1.close();
				stmt1.close();
				//System.out.println ("time stamp = " + timeStamp + "   users = "+ userSet.toString());
			}
			
			rs.close();
			stmt.close();
			c.close();
		}catch(Exception e){e.printStackTrace();}
		return result;
	}

	public static TreeMap< Long, HashSet<Integer>> getSslidingJoinUsersOfTimestaps(){
		
		TreeMap< Long, HashSet<Integer>> result = new TreeMap< Long, HashSet<Integer>>();
		Connection c = null;
		Statement stmt = null, stmt1 = null;
		try {		
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());	      
			c.setAutoCommit(false);
			stmt = c.createStatement();
			stmt1 = c.createStatement();
			
			String sql="SELECT DISTINCT SSJ.TIMESTAMP FROM SSJ ";
			ResultSet rs = stmt.executeQuery( sql);

			while ( rs.next() ) {
				Long timeStamp  = rs.getLong("TIMESTAMP");
				String sql1="SELECT SSJ.USERID FROM SSJ WHERE SSJ.TIMESTAMP = " + timeStamp ;
				ResultSet rs1 = stmt1.executeQuery( sql1);
				HashSet<Integer> userSet = new HashSet<Integer>() ;
				
				while ( rs1.next() ) {
					Integer userID  = rs1.getInt("USERID");
					userSet.add(userID);
				}
				result.put(timeStamp,userSet);
				rs1.close();
				stmt1.close();
				//System.out.println ("time stamp = " + timeStamp + "   users = "+ userSet.toString());
			}
			rs.close();
			stmt.close();
			c.close();
		}catch(Exception e){e.printStackTrace();}
		return result;
	}

	public static TreeMap< Long, HashSet<Integer>> getRJoinUsersOfTimestaps(){
		
		TreeMap< Long, HashSet<Integer>> result = new TreeMap< Long, HashSet<Integer>>();
		Connection c = null;
		Statement stmt = null, stmt1 = null;
		try {		
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());	      
			c.setAutoCommit(false);
			stmt = c.createStatement();
			stmt1 = c.createStatement();
			
			String sql="SELECT DISTINCT RJ.TIMESTAMP FROM RJ ";
			ResultSet rs = stmt.executeQuery( sql);

			
			while ( rs.next() ) {
				
				Long timeStamp  = rs.getLong("TIMESTAMP");
				String sql1="SELECT RJ.USERID FROM RJ WHERE RJ.TIMESTAMP = " + timeStamp ;
				ResultSet rs1 = stmt1.executeQuery( sql1);
				HashSet<Integer> userSet = new HashSet<Integer>() ;
				
				while ( rs1.next() ) {
					Integer userID  = rs1.getInt("USERID");
					userSet.add(userID);
				}
				
				result.put(timeStamp,userSet);
				rs1.close();
				stmt1.close();
			}
			rs.close();
			stmt.close();
			c.close();
		}catch(Exception e){e.printStackTrace();}
		return result;
	}

	public static TreeMap< Long, HashSet<Integer>> getDWJoinUsersOfTimestaps(){
		
		TreeMap< Long, HashSet<Integer>> result = new TreeMap< Long, HashSet<Integer>>();
		Connection c = null;
		Statement stmt = null, stmt1 = null;
		try {		
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());	      
			c.setAutoCommit(false);
			stmt = c.createStatement();
			stmt1 = c.createStatement();
			
			String sql="SELECT  DISTINCT DWJ.TIMESTAMP FROM DWJ ";
			ResultSet rs = stmt.executeQuery( sql);

			
			while ( rs.next() ) {
				Long timeStamp  = rs.getLong("TIMESTAMP");
				String sql1="SELECT DWJ.USERID FROM DWJ WHERE DWJ.TIMESTAMP = " + timeStamp ;
				ResultSet rs1 = stmt1.executeQuery( sql1);
				HashSet<Integer> userSet = new HashSet<Integer>() ;
				
				while ( rs1.next() ) {
					Integer userID  = rs1.getInt("USERID");
					userSet.add(userID);
				}
				
				result.put(timeStamp,userSet);
				rs1.close();
				stmt1.close();
				
			}
			rs.close();
			stmt.close();
			c.close();
		}catch(Exception e){e.printStackTrace();}
		return result;
	}

	public static TreeMap< Long, HashSet<Integer>> getLRUJoinUsersOfTimestaps(){
		
		TreeMap< Long, HashSet<Integer>> result = new TreeMap< Long, HashSet<Integer>>();
		Connection c = null;
		Statement stmt = null, stmt1 = null;
		try {		
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());	      
			c.setAutoCommit(false);
			stmt = c.createStatement();
			stmt1 = c.createStatement();
			
			String sql="SELECT DISTINCT LRUJ.TIMESTAMP FROM LRUJ ";
			ResultSet rs = stmt.executeQuery( sql);

			while ( rs.next() ) {
				Long timeStamp  = rs.getLong("TIMESTAMP");
				String sql1="SELECT LRUJ.USERID FROM LRUJ WHERE LRUJ.TIMESTAMP = " + timeStamp ;
				ResultSet rs1 = stmt1.executeQuery( sql1);
				HashSet<Integer> userSet = new HashSet<Integer>() ;
				
				while ( rs1.next() ) {
					Integer userID  = rs1.getInt("USERID");
					userSet.add(userID);
				}
				result.put(timeStamp,userSet);
				rs1.close();
				stmt1.close();
				//System.out.println ("time stamp = " + timeStamp + "   users = "+ userSet.toString());
			}
			rs.close();
			stmt.close();
			c.close();
		}catch(Exception e){e.printStackTrace();}
		return result;
	}

	public static TreeMap< Long, HashSet<Integer>> getPrefectSJoinUsersOfTimestaps(){
		
		TreeMap< Long, HashSet<Integer>> result = new TreeMap< Long, HashSet<Integer>>();
		Connection c = null;
		Statement stmt = null, stmt1 = null;
		try {		
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());	      
			c.setAutoCommit(false);
			stmt = c.createStatement();
			stmt1 = c.createStatement();
			
			String sql="SELECT DISTINCT SSpJ.TIMESTAMP FROM SSpJ ";
			ResultSet rs = stmt.executeQuery( sql);

			while ( rs.next() ) {
				Long timeStamp  = rs.getLong("TIMESTAMP");
				String sql1="SELECT SSpJ.USERID FROM SSpJ WHERE SSpJ.TIMESTAMP = " + timeStamp ;
				ResultSet rs1 = stmt1.executeQuery( sql1);
				HashSet<Integer> userSet = new HashSet<Integer>() ;
				
				while ( rs1.next() ) {
					Integer userID  = rs1.getInt("USERID");
					userSet.add(userID);
				}
				result.put(timeStamp,userSet);
				rs1.close();
				stmt1.close();
			//	System.out.println ("time stamp = " + timeStamp + "   users = "+ userSet.toString());
			}
			rs.close();
			stmt.close();
			c.close();
		}catch(Exception e){e.printStackTrace();}
		return result;
	}

	public static TreeMap< Long, HashSet<Integer>> getFJoinUsersOfTimestaps(){
		
		TreeMap< Long, HashSet<Integer>> result = new TreeMap< Long, HashSet<Integer>>();
		Connection c = null;
		Statement stmt = null, stmt1 = null;
		try {		
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());	      
			c.setAutoCommit(false);
			stmt = c.createStatement();
			stmt1 = c.createStatement();
			
			String sql="SELECT DISTINCT FJ.TIMESTAMP FROM FJ ";
			ResultSet rs = stmt.executeQuery( sql);

			while ( rs.next() ) {
				Long timeStamp  = rs.getLong("TIMESTAMP");
				String sql1="SELECT FJ.USERID FROM FJ WHERE FJ.TIMESTAMP = " + timeStamp ;
				ResultSet rs1 = stmt1.executeQuery( sql1);
				HashSet<Integer> userSet = new HashSet<Integer>() ;
				
				while ( rs1.next() ) {
					Integer userID  = rs1.getInt("USERID");
					userSet.add(userID);
				}
				result.put(timeStamp,userSet);
				rs1.close();
				stmt1.close();
			//	System.out.println ("time stamp = " + timeStamp + "   users = "+ userSet.toString());
			}
			rs.close();
			stmt.close();
			c.close();
		}catch(Exception e){e.printStackTrace();}
		return result;
	}

	public static HashMap<Long,Integer> computeErrors (TreeMap< Long, HashSet<Integer>> original , TreeMap< Long, HashSet<Integer>> replica){
		
		HashMap<Long,Integer> result=new HashMap<Long, Integer>();
		
		Iterator<Long> timeIt= original.keySet().iterator();
		int t = 0;
		
		while(timeIt.hasNext()){
			
			Integer error = 0;
			long timestamp = timeIt.next();
			
			// find same timestamps in original and replica
			HashSet<Integer> originalUsers = original.get(timestamp);
			HashSet<Integer> replicaUsers = new HashSet<Integer>();
			if (replica.containsKey(timestamp)) {
				replicaUsers = replica.get(timestamp);
			}else { 
				continue; 
			}
			
			// for users exist in original but not exist in replica
			Iterator<Integer> originalUserIt= originalUsers.iterator();
			while(originalUserIt.hasNext()){
				Integer userId = originalUserIt.next();
				if( !replicaUsers.contains(userId)){
					error ++;
				}
			}
			
			// for users exist in replica but not exist in original
			Iterator<Integer> replicaUserIt= replicaUsers.iterator();
			while(replicaUserIt.hasNext()){
				Integer userId = replicaUserIt.next();
				if( !originalUsers.contains(userId)){
					error ++;
				}
			}
			t++;
			result.put(timestamp, error);
			if ( t< 10){
			//System.out.println("time stamp = " + timestamp );
			//System.out.println("\noriginal users = " + originalUsers.toString() );
			//System.out.println("replica users =  " + replicaUsers.toString() );
			//System.out.println("error" + error + "\n\n\n");
			}
			
		}
		return result;
		
	}

	public static HashMap<Long,Double> computeErrorsJaccardIndex (TreeMap< Long, HashSet<Integer>> original , TreeMap< Long, HashSet<Integer>> replica){
	
	HashMap<Long,Double> result=new HashMap<Long, Double>();
	
	Iterator<Long> timeIt= original.keySet().iterator();
	while(timeIt.hasNext()){
		
		Double intersec = 0.0 , jaccard = 0.0;
		long timestamp = timeIt.next();
		
		// find same timestamps in original and replica
		HashSet<Integer> originalUsers = original.get(timestamp);
		HashSet<Integer> replicaUsers = new HashSet<Integer>();
		if (replica.containsKey(timestamp)) {
			replicaUsers = replica.get(timestamp);
		}else { 
			continue; 
		}
		
		// compute  size of ( original intersection replica )
		Iterator<Integer> originalUserIt= originalUsers.iterator();
		while(originalUserIt.hasNext()){
			Integer userId = originalUserIt.next();
			if( replicaUsers.contains(userId)){
				intersec ++;
			}
		}
		
		// compute Jaccard index
		jaccard = 1 - (intersec / (originalUsers.size() + replicaUsers.size() - intersec ) ) ;
		
		result.put(timestamp, jaccard);
		
		
	}
	return result;
	
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
	
	
	/*public static void main(String[] args){
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
				Integer cOC=0, cdwe=0, cse=0, cre=0, cbe=0, csse=0, cspe=0, csspe=0, clrunle=0, crnle=0, cgnre=0, clrue=0, cfe=0, csce=0;
				
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
					Integer lrue=PWSError.get(nextTime);
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
					clrue= clrue + (lrue= lrue==null?0:lrue) ;
					cfe= cfe + (fe= fe==null?0:fe) ;
					csce= csce + (sce= sce==null?0:sce) ;
					
					bw.write(nextTime+","+OC+","+(dwe==null?0:dwe)+","+(se==null?0:se)+","+(re==null?0:re)+","+(be==null?0:be)+","+(sse==null?0:sse)+","+(spe==null?0:spe)+","+(sspe==null?0:sspe)+","+(lrunle==null?0:lrunle)+","+(rnle==null?0:rnle)+","+(gnre==null?0:gnre)+","+(lrue==null?0:lrue)+","+(fe==null?0:fe)+","+(sce==null?0:sce)+",");
					bw.write(cOC+","+(cdwe==null?0:cdwe)+","+(cse==null?0:cse)+","+(cre==null?0:cre)+","+(cbe==null?0:cbe)+","+(csse==null?0:csse)+","+(cspe==null?0:cspe)+","+(csspe==null?0:csspe)+","+(clrunle==null?0:clrunle)+","+(crnle==null?0:crnle)+","+(cgnre==null?0:cgnre)+","+(clrue==null?0:clrue)+","+(cfe==null?0:cfe)+","+(csce==null?0:csce)+"\n");
				}
				bw.flush();
				bw.close();
			}catch(Exception e){e.printStackTrace();}
		}*/
}
