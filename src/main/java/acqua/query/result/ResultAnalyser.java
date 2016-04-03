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
			
			stmt.executeUpdate(" DROP TABLE IF EXISTS OJ ;");
			String sql = "CREATE TABLE  `OJ` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `OtimeIndex` ON `OJ` (`TIMESTAMP` ASC);"; 
			//System.out.println(sql);
			stmt.executeUpdate(sql);

			stmt.executeUpdate(" DROP TABLE IF EXISTS WSTJ ;");
			sql = "CREATE TABLE  `WSTJ` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `WSTtimeIndex` ON `WSTJ` (`TIMESTAMP` ASC);"; 
			//System.out.println(sql);
			stmt.executeUpdate(sql);
			
			
			stmt.executeUpdate(" DROP TABLE IF EXISTS LRUJ ;");
			sql = "CREATE TABLE  `LRUJ` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `LRUtimeIndex` ON `LRUJ` (`TIMESTAMP` ASC);"; 
			//System.out.println(sql);
			stmt.executeUpdate(sql);
			
			stmt.executeUpdate(" DROP TABLE IF EXISTS RNDJ ;");
			sql = "CREATE TABLE  `RNDJ` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `RNDtimeIndex` ON `RNDJ` (`TIMESTAMP` ASC);"; 
			//System.out.println(sql);
			stmt.executeUpdate(sql);
			
			stmt.executeUpdate(" DROP TABLE IF EXISTS WBMJ ;");
			sql = "CREATE TABLE  `WBMJ` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `WBMtimeIndex` ON `WBMJ` (`TIMESTAMP` ASC);"; 
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
			
			stmt.executeUpdate(" DROP TABLE IF EXISTS SCJ ;");
			sql = "CREATE TABLE  `SCJ` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `SCtimeIndex` ON `SCJ` (`TIMESTAMP` ASC);"; 
			//System.out.println(sql);
			stmt.executeUpdate(sql);
			
			///////////////////////////////////////////////////////// Simple combine     //////////////////////////////////////////////////////////

			stmt.executeUpdate(" DROP TABLE IF EXISTS LRUFJ ;");
			sql = "CREATE TABLE  `LRUFJ` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `LRUFtimeIndex` ON `LRUFJ` (`TIMESTAMP` ASC);"; 
			//System.out.println(sql);
			stmt.executeUpdate(sql);
			stmt.executeUpdate(" DROP TABLE IF EXISTS RNDFJ ;");
			sql = "CREATE TABLE  `RNDFJ` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `RNDFtimeIndex` ON `RNDFJ` (`TIMESTAMP` ASC);"; 
			//System.out.println(sql);
			stmt.executeUpdate(sql);
			stmt.executeUpdate(" DROP TABLE IF EXISTS WBMFJ ;");
			sql = "CREATE TABLE  `WBMFJ` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `WBMFtimeIndex` ON `WBMFJ` (`TIMESTAMP` ASC);"; 
			//System.out.println(sql);
			stmt.executeUpdate(sql);
			
			///////////////////////////////////////////////////////// TA combine     //////////////////////////////////////////////////////////
			stmt.executeUpdate(" DROP TABLE IF EXISTS LRUFTAJ ;");
			sql = "CREATE TABLE  `LRUFTAJ` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `LRUFTAtimeIndex` ON `LRUFTAJ` (`TIMESTAMP` ASC);"; 
			//System.out.println(sql);
			stmt.executeUpdate(sql);
			stmt.executeUpdate(" DROP TABLE IF EXISTS RNDFTAJ ;");
			sql = "CREATE TABLE  `RNDFTAJ` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `RNDFTAtimeIndex` ON `RNDFTAJ` (`TIMESTAMP` ASC);"; 
			//System.out.println(sql);
			stmt.executeUpdate(sql);
			stmt.executeUpdate(" DROP TABLE IF EXISTS WBMFTAJ ;");
			sql = "CREATE TABLE  `WBMFTAJ` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); CREATE INDEX `WBMTAFtimeIndex` ON `WBMFTAJ` (`TIMESTAMP` ASC);"; 
			//System.out.println(sql);
			stmt.executeUpdate(sql);
			
			InputStream    fis;
			BufferedReader br;

			

			//-----------------------------------------------------------------------fill classqueryProcessorOracleJoinOperatorOutput
			fis = new FileInputStream(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/OracleJoinOperatorOutput.txt");
			br = new BufferedReader(new InputStreamReader(fis));
			String line=null;
			while((line=br.readLine())!=null)
			{
				String[] userInfo = line.split(" ");	
				sql = "INSERT INTO OJ (USERID,MENTIONCOUNT,FOLLOWERCOUNT,TIMESTAMP) " +
						"VALUES ("+userInfo[0]+","+userInfo[1]+","+userInfo[2]+","+userInfo[3]+")"; 
				//System.out.println(sql);
				stmt.executeUpdate(sql);
			}
			
			//---------------------------------------------------------------------fill DWJ
			fis = new FileInputStream(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/WSTJoinOperatorOutput.txt");
			br = new BufferedReader(new InputStreamReader(fis));
			line=null;
			while((line=br.readLine())!=null)
			{
				String[] userInfo = line.split(" ");	
				sql = "INSERT INTO WSTJ (USERID,MENTIONCOUNT,FOLLOWERCOUNT,TIMESTAMP) " +
						"VALUES ("+userInfo[0]+","+userInfo[1]+","+userInfo[2]+","+userInfo[3]+")"; 
				//System.out.println(sql);
				stmt.executeUpdate(sql);
			}

			//---------------------------------------------------------------------fill baseline table
			
			fis = new FileInputStream(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/RNDJoinOperatorOutput.txt");
			br = new BufferedReader(new InputStreamReader(fis));
			line=null;
			while((line=br.readLine())!=null)
			{
				String[] userInfo = line.split(" ");	
				sql = "INSERT INTO RNDJ (USERID,MENTIONCOUNT,FOLLOWERCOUNT,TIMESTAMP) " +
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
			
			//---------------------------------------------------------------------fill WBMJ

			fis = new FileInputStream(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/WBMJoinOperatorOutput.txt");
			br = new BufferedReader(new InputStreamReader(fis));
			line=null;
			while((line=br.readLine())!=null)
			{
				String[] userInfo = line.split(" ");	
				sql = "INSERT INTO WBMJ (USERID,MENTIONCOUNT,FOLLOWERCOUNT,TIMESTAMP) " +
						"VALUES ("+userInfo[0]+","+userInfo[1]+","+userInfo[2]+","+userInfo[3]+")"; 
				//System.out.println(sql);
				stmt.executeUpdate(sql);
			}
			
			//---------------------------------------------------------------------fill FJ
			fis = new FileInputStream(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/FilterJoinOperatorOutput.txt");
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
//			fis = new FileInputStream(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/ScoringJoinOperatorOutput.txt");
//			br = new BufferedReader(new InputStreamReader(fis));
//			line=null;
//			while((line=br.readLine())!=null)
//			{
//				String[] userInfo = line.split(" ");	
//				sql = "INSERT INTO SCJ (USERID,MENTIONCOUNT,FOLLOWERCOUNT,TIMESTAMP) " +
//						"VALUES ("+userInfo[0]+","+userInfo[1]+","+userInfo[2]+","+userInfo[3]+")"; 
//				//System.out.println(sql);
//				stmt.executeUpdate(sql);
//			}
//			
			
			//////////////////////////////////////////// Simple combine ////////////////////////////////////////////////////////////////////////
			
			//---------------------------------------------------------------------fill LRUFJ
			fis = new FileInputStream(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/LRUFOperatorOutput.txt");
			br = new BufferedReader(new InputStreamReader(fis));
			line=null;
			while((line=br.readLine())!=null)
			{
				String[] userInfo = line.split(" ");	
				sql = "INSERT INTO LRUFJ (USERID,MENTIONCOUNT,FOLLOWERCOUNT,TIMESTAMP) " +
						"VALUES ("+userInfo[0]+","+userInfo[1]+","+userInfo[2]+","+userInfo[3]+")"; 
				//System.out.println(sql);
				stmt.executeUpdate(sql);
			}
			//---------------------------------------------------------------------fill RNDFJ
			fis = new FileInputStream(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/RNDFOperatorOutput.txt");
			br = new BufferedReader(new InputStreamReader(fis));
			line=null;
			while((line=br.readLine())!=null)
			{
				String[] userInfo = line.split(" ");	
				sql = "INSERT INTO RNDFJ (USERID,MENTIONCOUNT,FOLLOWERCOUNT,TIMESTAMP) " +
						"VALUES ("+userInfo[0]+","+userInfo[1]+","+userInfo[2]+","+userInfo[3]+")"; 
				//System.out.println(sql);
				stmt.executeUpdate(sql);
			}
			//---------------------------------------------------------------------fill WBMFJ
			fis = new FileInputStream(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/WBMFOperatorOutput.txt");
			br = new BufferedReader(new InputStreamReader(fis));
			line=null;
			while((line=br.readLine())!=null)
			{
				String[] userInfo = line.split(" ");	
				sql = "INSERT INTO WBMFJ (USERID,MENTIONCOUNT,FOLLOWERCOUNT,TIMESTAMP) " +
						"VALUES ("+userInfo[0]+","+userInfo[1]+","+userInfo[2]+","+userInfo[3]+")"; 
				//System.out.println(sql);
				stmt.executeUpdate(sql);
			}
			
			//////////////////////////////////////////// TA combine ////////////////////////////////////////////////////////////////////////
			
			
			//---------------------------------------------------------------------fill LRUFTAJ
			fis = new FileInputStream(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/LRUFTAOperatorOutput.txt");
			br = new BufferedReader(new InputStreamReader(fis));
			line=null;
			while((line=br.readLine())!=null)
			{
				String[] userInfo = line.split(" ");	
				sql = "INSERT INTO LRUFTAJ (USERID,MENTIONCOUNT,FOLLOWERCOUNT,TIMESTAMP) " +
						"VALUES ("+userInfo[0]+","+userInfo[1]+","+userInfo[2]+","+userInfo[3]+")"; 
				//System.out.println(sql);
				stmt.executeUpdate(sql);
			}
			//---------------------------------------------------------------------fill RNDFTAJ
			fis = new FileInputStream(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/RNDFTAOperatorOutput.txt");
			br = new BufferedReader(new InputStreamReader(fis));
			line=null;
			while((line=br.readLine())!=null)
			{
				String[] userInfo = line.split(" ");	
				sql = "INSERT INTO RNDFTAJ (USERID,MENTIONCOUNT,FOLLOWERCOUNT,TIMESTAMP) " +
						"VALUES ("+userInfo[0]+","+userInfo[1]+","+userInfo[2]+","+userInfo[3]+")"; 
				//System.out.println(sql);
				stmt.executeUpdate(sql);
			}
			//---------------------------------------------------------------------fill WBMFTAJ
			fis = new FileInputStream(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/WBMFTAOperatorOutput.txt");
			br = new BufferedReader(new InputStreamReader(fis));
			line=null;
			while((line=br.readLine())!=null)
			{
				String[] userInfo = line.split(" ");	
				sql = "INSERT INTO WBMFTAJ (USERID,MENTIONCOUNT,FOLLOWERCOUNT,TIMESTAMP) " +
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

	
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//Functions related to Top-k Aqua
	
	public static void analysisExperimentJaccard(){
		
		try{
			insertResultToDB();
			BufferedWriter bw=new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare.csv")));
			TreeMap<Long,Integer> oracleCount=computeOJoin();

			//use Jaccard Index for computing errors
			HashMap<Long,Double> WBMError=computeErrorsJaccardIndex(getOracleUsersOfTimestaps() , getWBMJoinUsersOfTimestaps() );
			HashMap<Long,Double> RNDError=computeErrorsJaccardIndex (getOracleUsersOfTimestaps() , getRNDJoinUsersOfTimestaps() );
			HashMap<Long,Double> WSTError=computeErrorsJaccardIndex (getOracleUsersOfTimestaps() , getWSTJoinUsersOfTimestaps() );
			HashMap<Long,Double> LRUError=computeErrorsJaccardIndex (getOracleUsersOfTimestaps() , getLRUJoinUsersOfTimestaps() );			
			HashMap<Long,Double> FError=computeErrorsJaccardIndex(getOracleUsersOfTimestaps() , getFJoinUsersOfTimestaps() );
			HashMap<Long,Double> LRUFError=computeErrorsJaccardIndex(getOracleUsersOfTimestaps() , getLRUFJoinUsersOfTimestaps() );
			HashMap<Long,Double> RNDFError=computeErrorsJaccardIndex(getOracleUsersOfTimestaps() , getRNDFJoinUsersOfTimestaps() );
			HashMap<Long,Double> WBMFError=computeErrorsJaccardIndex(getOracleUsersOfTimestaps() , getWBMFJoinUsersOfTimestaps() );
			HashMap<Long,Double> LRUFTAError=computeErrorsJaccardIndex(getOracleUsersOfTimestaps() , getLRUFTAJoinUsersOfTimestaps() );
			HashMap<Long,Double> RNDFTAError=computeErrorsJaccardIndex(getOracleUsersOfTimestaps() , getRNDFTAJoinUsersOfTimestaps() );
			HashMap<Long,Double> WBMFTAError=computeErrorsJaccardIndex(getOracleUsersOfTimestaps() , getWBMFTAJoinUsersOfTimestaps() );
			
			Iterator<Long> itO = oracleCount.keySet().iterator();
			bw.write("timestampe,Oracle,WST,RND,WBM,LRU,Filter,LRU.F,RND.F,WBM.F,LRU.F.TA,RND.F.TA,WBM.F.TA\n");

			Double cOC=0.0, cwste=0.0, crnde=0.0,cwbme=0.0, clrue=0.0, cfe=0.0, cspe=0.0 ,clrufe =0.0 ,crndfe =0.0 ,cwbmfe =0.0,clruftae =0.0 ,crndftae =0.0 ,cwbmftae =0.0;
			while(itO.hasNext()){
				
				long nextTime = itO.next();
				Integer OC=oracleCount.get(nextTime);
				Double wste=WSTError.get(nextTime);
				Double rnde=RNDError.get(nextTime);
				Double wbme=WBMError.get(nextTime);
				Double lrue=LRUError.get(nextTime);
				Double fe=FError.get(nextTime);
				Double lrufe = LRUFError.get(nextTime);
				Double rndfe = RNDFError.get(nextTime);
				Double wbmfe = WBMFError.get(nextTime);
				Double lruftae = LRUFTAError.get(nextTime);
				Double rndftae = RNDFTAError.get(nextTime);
				Double wbmftae = WBMFTAError.get(nextTime);

				//cumulative error
				cOC=cOC + OC ; 
				cwste= cwste + (wste = wste==null?0:wste) ;
				crnde= crnde + (rnde = rnde==null?0:rnde) ;
				cwbme= cwbme + (wbme= wbme==null?0:wbme) ;
				clrue= clrue + (lrue= lrue==null?0:lrue) ;
				cfe= cfe + (fe= fe==null?0:fe) ;
				clrufe = clrufe + (lrufe= lrufe==null?0:lrufe) ;
				crndfe = crndfe + (rndfe= rndfe==null?0:rndfe) ;
				cwbmfe = cwbmfe + (wbmfe= wbmfe==null?0:wbmfe) ;
				clruftae = clruftae + (lruftae= lruftae==null?0:lruftae) ;
				crndftae = crndftae + (rndftae= rndftae==null?0:rndftae) ;
				cwbmftae = cwbmftae + (wbmftae= wbmftae==null?0:wbmftae) ;
				
				
				bw.write(nextTime+","+String.format("%.2f",cOC)+
						","+ String.format("%.2f",(cwste==null?0:cwste))+ 
						","+ String.format("%.2f",(crnde==null?0:crnde))+
						","+ String.format("%.2f",(cwbme==null?0:cwbme)) +
						","+ String.format("%.2f",(clrue==null?0:clrue)) +
						","+ String.format("%.2f",(cfe==null?0:cfe))+
						","+ String.format("%.2f",(clrufe==null?0:clrufe))+
						","+ String.format("%.2f",(crndfe==null?0:crndfe))+
						","+ String.format("%.2f",(cwbmfe==null?0:cwbmfe))+
						","+ String.format("%.2f",(clruftae==null?0:clruftae))+
						","+ String.format("%.2f",(crndftae==null?0:crndftae))+
						","+ String.format("%.2f",(cwbmftae==null?0:cwbmftae))+"\n");

				
			}
			bw.flush();
			bw.close();
		}catch(Exception e){e.printStackTrace();}
		
	}

	public static void analysisMultipleExperimentsJaccard(int index ){
		
		try{
			
			BufferedWriter bw=new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compareMultipleExperiments.csv")));

			bw.write("timestampe,Oracle Min,WST Min,RND Min,WBM Min,LRU Min,Filter Min,LRU.F Min,RND.F Min,WBM.F Min,LRU.F.TA Min,RND.F.TA Min,WBM.F.TA Min,"
							  + "Oracle Max,WST Min,RND Max,WBM Max,LRU Max,Filter Max,LRU.F Max,RND.F Max,WBM.F Max,LRU.F.TA Max,RND.F.TA Max,WBM.F.TA Max,"
							  + "Oracle Avg,WST Avg,RND Avg,WBM Avg,LRU Avg,Filter Avg,LRU.F Avg,RND.F Avg,WBM.F Avg,LRU.F.TA Avg,RND.F.TA Avg,WBM.F.TA Avg\n");
			
			
			//Min variables
			Double OCMin = Double.MAX_VALUE, wsteMin = Double.MAX_VALUE, rndeMin = Double.MAX_VALUE, wbmeMin = Double.MAX_VALUE, lrueMin = Double.MAX_VALUE, speMin = Double.MAX_VALUE;
			Double feMin = Double.MAX_VALUE,lrufeMin =Double.MAX_VALUE ,rndfeMin =Double.MAX_VALUE ,wbmfeMin =Double.MAX_VALUE,lruftaeMin =Double.MAX_VALUE ,rndftaeMin =Double.MAX_VALUE ,wbmftaeMin =Double.MAX_VALUE;
			//Max variables
			Double OCMax = 0.0, wsteMax= 0.0, rndeMax = 0.0,wbmeMax = 0.0, lrueMax = 0.0, feMax = 0.0,lrufeMax =0.0 ,rndfeMax =0.0 ,wbmfeMax =0.0,lruftaeMax =0.0 ,rndftaeMax =0.0 ,wbmftaeMax =0.0;
			//Average variables 
			Double OCAvg = 0.0, wsteAvg = 0.0, rndeAvg = 0.0, wbmeAvg = 0.0, lrueAvg = 0.0, feAvg = 0.0,lrufeAvg =0.0 ,rndfeAvg =0.0 ,wbmfeAvg =0.0,lruftaeAvg =0.0 ,rndftaeAvg =0.0 ,wbmftaeAvg =0.0;
			//Sum variables 
			Double OCSum = 0.0, wsteSum = 0.0, rndeSum = 0.0, wbmeSum = 0.0, lrueSum = 0.0, feSum = 0.0,lrufeSum =0.0 ,rndfeSum =0.0 ,wbmfeSum =0.0,lruftaeSum =0.0 ,rndftaeSum =0.0 ,wbmftaeSum =0.0;
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
					Double wste = Double.parseDouble(lineSplit[2]);
					Double rnde = Double.parseDouble(lineSplit[3]);
					Double wbme = Double.parseDouble(lineSplit[4]);
					Double lrue = Double.parseDouble(lineSplit[5]);
					Double fe = Double.parseDouble(lineSplit[6]);
					Double lrufe = Double.parseDouble(lineSplit[7]);
					Double rndfe = Double.parseDouble(lineSplit[8]);
					Double wbmfe = Double.parseDouble(lineSplit[9]);
					Double lruftae = Double.parseDouble(lineSplit[10]);
					Double rndftae = Double.parseDouble(lineSplit[11]);
					Double wbmftae = Double.parseDouble(lineSplit[12]);
				
					if (OC < OCMin )  OCMin = OC;
					if (wste < wsteMin )  wsteMin = wste;
					if (rnde < rndeMin )  rndeMin = rnde;
					if (wbme < wbmeMin )  wbmeMin = wbme;
					if (lrue < lrueMin )  lrueMin = lrue;
					if (fe < feMin )  feMin = fe;
					if (lrufe < lrufeMin )  lrufeMin = lrufe;
					if (rndfe < rndfeMin )  rndfeMin = rndfe;
					if (wbmfe < wbmfeMin )  wbmfeMin = wbmfe;
					if (lruftae < lruftaeMin )  lruftaeMin = lruftae;
					if (rndftae < rndftaeMin )  rndftaeMin = rndftae;
					if (wbmftae < wbmftaeMin )  wbmftaeMin = wbmftae;
					
					if (OC > OCMax )  OCMax = OC;
					if (wste > wsteMax )  wsteMax = wste;
					if (rnde > rndeMax )  rndeMax = rnde;
					if (wbme > wbmeMax )  wbmeMax = wbme;
					if (lrue > lrueMax )  lrueMax = lrue;
					if (fe > feMax )  feMax = fe;
					if (lrufe > lrufeMax )  lrufeMax = lrufe;
					if (rndfe > rndfeMax )  rndfeMax = rndfe;
					if (wbmfe > wbmfeMax )  wbmfeMax = wbmfe;
					if (lruftae > lruftaeMax )  lruftaeMax = lruftae;
					if (rndftae > rndftaeMax )  rndftaeMax = rndftae;
					if (wbmftae > wbmftaeMax )  wbmftaeMax = wbmftae;
	
					OCSum += OC;
					wsteSum += wste;
					System.out.println("wst error =  " + wste);
					rndeSum += rnde;
					wbmeSum += wbme;
					lrueSum += lrue;
					//speSum +=spe;
					feSum += fe;
					lrufeSum += lrufe;
					rndfeSum += rndfe;
					wbmfeSum += wbmfe;
					lruftaeSum += lruftae;
					rndftaeSum += rndftae;
					wbmftaeSum += wbmftae;
				
				}
			
				int totalNum = Config.INSTANCE.getDatabaseNumber() - nullLine;
				
				OCAvg = OCSum / totalNum;
				wsteAvg = wsteSum / totalNum;
				rndeAvg = rndeSum / totalNum;
				wbmeAvg = wbmeSum / totalNum;
				lrueAvg = lrueSum / totalNum;
				feAvg = feSum / totalNum;
				lrufeAvg = lrufeSum / totalNum;
				rndfeAvg = rndfeSum / totalNum;
				wbmfeAvg = wbmfeSum / totalNum;
				lruftaeAvg = lruftaeSum / totalNum;
				rndftaeAvg = rndftaeSum / totalNum;
				wbmftaeAvg = wbmftaeSum / totalNum;
				
				
				bw.write(nextTime+","+OCMin+","+
						(wsteMin==null?0:wsteMin)+","+
						(rndeMin==null?0:rndeMin)+","+
						(wbmeMin==null?0:wbmeMin)+"," +
						(lrueMin==null?0:lrueMin)+","+
						(feMin==null?0:feMin)+","+
						(lrufeMin==null?0:lrufeMin)+","+
						(rndfeMin==null?0:rndfeMin)+","+
						(wbmfeMin==null?0:wbmfeMin)+","+
						(lruftaeMin==null?0:lruftaeMin)+","+
						(rndftaeMin==null?0:rndftaeMin)+","+
						(wbmftaeMin==null?0:wbmftaeMin)+","+
						OCMax+","+
						(wsteMax==null?0:wsteMax)+","+
						(rndeMax==null?0:rndeMax)+","+
						(wbmeMax==null?0:wbmeMax)+"," +
						(lrueMax==null?0:lrueMax)+","+
						(feMax==null?0:feMax)+","+
						(lrufeMax==null?0:lrufeMax)+","+
						(rndfeMax==null?0:rndfeMax)+","+
						(wbmfeMax==null?0:wbmfeMax)+","+
						(lruftaeMax==null?0:lruftaeMax)+","+
						(rndftaeMax==null?0:rndftaeMax)+","+
						(wbmftaeMax==null?0:wbmftaeMax)+","+
						OCAvg+","+
						(wsteAvg)+","+
						(rndeAvg)+","+
						(wbmeAvg)+"," +
						(lrueAvg)+","+
						(feAvg)+","+
						(lrufeAvg)+","+
						(rndfeAvg)+","+
						(wbmfeAvg)+","+
						(lruftaeAvg)+","+
						(rndftaeAvg)+","+
						(wbmftaeAvg)+"\n");
	
				OCMin = Double.MAX_VALUE; wsteMin = Double.MAX_VALUE; rndeMin = Double.MAX_VALUE; wbmeMin = Double.MAX_VALUE; lrueMin = Double.MAX_VALUE; speMin =Double.MAX_VALUE;  
				feMin = Double.MAX_VALUE;lrufeMin =Double.MAX_VALUE ;rndfeMin =Double.MAX_VALUE ;wbmfeMin =Double.MAX_VALUE;lruftaeMin =Double.MAX_VALUE ;rndftaeMin =Double.MAX_VALUE ;wbmftaeMin =Double.MAX_VALUE;
				OCMax = 0.0; wsteMax = 0.0; rndeMax = 0.0; wbmeMax = 0.0; lrueMax = 0.0; feMax = 0.0; lrufeMax =0.0; rndfeMax =0.0; wbmfeMax =0.0;lruftaeMax =0.0; rndftaeMax =0.0; wbmftaeMax =0.0;
				OCAvg = 0.0; wsteAvg = 0.0; rndeAvg = 0.0; wbmeAvg = 0.0; lrueAvg = 0.0; feAvg = 0.0; lrufeAvg =0.0; rndfeAvg =0.0; wbmfeAvg =0.0;lruftaeAvg =0.0; rndftaeAvg =0.0; wbmftaeAvg =0.0;
				OCSum = 0.0; wsteSum = 0.0; rndeSum = 0.0; wbmeSum = 0.0; lrueSum = 0.0; feSum = 0.0; lrufeSum =0.0; rndfeSum =0.0; wbmfeSum =0.0;lruftaeSum =0.0; rndftaeSum =0.0; wbmftaeSum =0.0;
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
				System.out.println ("time stamp = " + timeStamp + "   users = "+ userSet.toString());
			}
			
			rs.close();
			stmt.close();
			c.close();
		}catch(Exception e){e.printStackTrace();}
		return result;
	}

	public static TreeMap< Long, HashSet<Integer>> getWBMJoinUsersOfTimestaps(){
		
		TreeMap< Long, HashSet<Integer>> result = new TreeMap< Long, HashSet<Integer>>();
		Connection c = null;
		Statement stmt = null, stmt1 = null;
		try {		
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());	      
			c.setAutoCommit(false);
			stmt = c.createStatement();
			stmt1 = c.createStatement();
			
			String sql="SELECT DISTINCT WBMJ.TIMESTAMP FROM WBMJ ";
			ResultSet rs = stmt.executeQuery( sql);

			while ( rs.next() ) {
				Long timeStamp  = rs.getLong("TIMESTAMP");
				String sql1="SELECT WBMJ.USERID FROM WBMJ WHERE WBMJ.TIMESTAMP = " + timeStamp ;
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

	public static TreeMap< Long, HashSet<Integer>> getRNDJoinUsersOfTimestaps(){
		
		TreeMap< Long, HashSet<Integer>> result = new TreeMap< Long, HashSet<Integer>>();
		Connection c = null;
		Statement stmt = null, stmt1 = null;
		try {		
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());	      
			c.setAutoCommit(false);
			stmt = c.createStatement();
			stmt1 = c.createStatement();
			
			String sql="SELECT DISTINCT RNDJ.TIMESTAMP FROM RNDJ ";
			ResultSet rs = stmt.executeQuery( sql);

			
			while ( rs.next() ) {
				
				Long timeStamp  = rs.getLong("TIMESTAMP");
				String sql1="SELECT RNDJ.USERID FROM RNDJ WHERE RNDJ.TIMESTAMP = " + timeStamp ;
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

	public static TreeMap< Long, HashSet<Integer>> getWSTJoinUsersOfTimestaps(){
		
		TreeMap< Long, HashSet<Integer>> result = new TreeMap< Long, HashSet<Integer>>();
		Connection c = null;
		Statement stmt = null, stmt1 = null;
		try {		
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());	      
			c.setAutoCommit(false);
			stmt = c.createStatement();
			stmt1 = c.createStatement();
			
			String sql="SELECT  DISTINCT WSTJ.TIMESTAMP FROM WSTJ ";
			ResultSet rs = stmt.executeQuery( sql);

			
			while ( rs.next() ) {
				Long timeStamp  = rs.getLong("TIMESTAMP");
				String sql1="SELECT WSTJ.USERID FROM WSTJ WHERE WSTJ.TIMESTAMP = " + timeStamp ;
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
			
			String sql="SELECT DISTINCT WBMJ.TIMESTAMP FROM WBMJ ";
			ResultSet rs = stmt.executeQuery( sql);

			while ( rs.next() ) {
				Long timeStamp  = rs.getLong("TIMESTAMP");
				String sql1="SELECT WBMJ.USERID FROM WBMJ WHERE WBMJ.TIMESTAMP = " + timeStamp ;
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
	
	public static TreeMap< Long, HashSet<Integer>> getLRUFJoinUsersOfTimestaps(){
		
		TreeMap< Long, HashSet<Integer>> result = new TreeMap< Long, HashSet<Integer>>();
		Connection c = null;
		Statement stmt = null, stmt1 = null;
		try {		
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());	      
			c.setAutoCommit(false);
			stmt = c.createStatement();
			stmt1 = c.createStatement();
			
			String sql="SELECT DISTINCT LRUFJ.TIMESTAMP FROM LRUFJ ";
			ResultSet rs = stmt.executeQuery( sql);

			while ( rs.next() ) {
				Long timeStamp  = rs.getLong("TIMESTAMP");
				String sql1="SELECT LRUFJ.USERID FROM LRUFJ WHERE LRUFJ.TIMESTAMP = " + timeStamp ;
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

	public static TreeMap< Long, HashSet<Integer>> getRNDFJoinUsersOfTimestaps(){
		
		TreeMap< Long, HashSet<Integer>> result = new TreeMap< Long, HashSet<Integer>>();
		Connection c = null;
		Statement stmt = null, stmt1 = null;
		try {		
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());	      
			c.setAutoCommit(false);
			stmt = c.createStatement();
			stmt1 = c.createStatement();
			
			String sql="SELECT DISTINCT RNDFJ.TIMESTAMP FROM RNDFJ ";
			ResultSet rs = stmt.executeQuery( sql);

			while ( rs.next() ) {
				Long timeStamp  = rs.getLong("TIMESTAMP");
				String sql1="SELECT RNDFJ.USERID FROM RNDFJ WHERE RNDFJ.TIMESTAMP = " + timeStamp ;
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

	public static TreeMap< Long, HashSet<Integer>> getWBMFJoinUsersOfTimestaps(){
	
	TreeMap< Long, HashSet<Integer>> result = new TreeMap< Long, HashSet<Integer>>();
	Connection c = null;
	Statement stmt = null, stmt1 = null;
	try {		
		Class.forName("org.sqlite.JDBC");
		c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());	      
		c.setAutoCommit(false);
		stmt = c.createStatement();
		stmt1 = c.createStatement();
		
		String sql="SELECT DISTINCT WBMFJ.TIMESTAMP FROM WBMFJ ";
		ResultSet rs = stmt.executeQuery( sql);

		while ( rs.next() ) {
			Long timeStamp  = rs.getLong("TIMESTAMP");
			String sql1="SELECT WBMFJ.USERID FROM WBMFJ WHERE WBMFJ.TIMESTAMP = " + timeStamp ;
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

	public static TreeMap< Long, HashSet<Integer>> getLRUFTAJoinUsersOfTimestaps(){
		
		TreeMap< Long, HashSet<Integer>> result = new TreeMap< Long, HashSet<Integer>>();
		Connection c = null;
		Statement stmt = null, stmt1 = null;
		try {		
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());	      
			c.setAutoCommit(false);
			stmt = c.createStatement();
			stmt1 = c.createStatement();
			
			String sql="SELECT DISTINCT LRUFTAJ.TIMESTAMP FROM LRUFTAJ ";
			ResultSet rs = stmt.executeQuery( sql);

			while ( rs.next() ) {
				Long timeStamp  = rs.getLong("TIMESTAMP");
				String sql1="SELECT LRUFTAJ.USERID FROM LRUFTAJ WHERE LRUFTAJ.TIMESTAMP = " + timeStamp ;
				ResultSet rs1 = stmt1.executeQuery( sql1);
				HashSet<Integer> userSet = new HashSet<Integer>() ;
				
				while ( rs1.next() ) {
					Integer userID  = rs1.getInt("USERID");
					userSet.add(userID);
				}
				result.put(timeStamp,userSet);
				rs1.close();
				stmt1.close();
				System.out.println ("time stamp = " + timeStamp + "   users = "+ userSet.toString());
			}
			rs.close();
			stmt.close();
			c.close();
		}catch(Exception e){e.printStackTrace();}
		return result;
	}

	public static TreeMap< Long, HashSet<Integer>> getRNDFTAJoinUsersOfTimestaps(){
		
		TreeMap< Long, HashSet<Integer>> result = new TreeMap< Long, HashSet<Integer>>();
		Connection c = null;
		Statement stmt = null, stmt1 = null;
		try {		
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());	      
			c.setAutoCommit(false);
			stmt = c.createStatement();
			stmt1 = c.createStatement();
			
			String sql="SELECT DISTINCT RNDFTAJ.TIMESTAMP FROM RNDFTAJ ";
			ResultSet rs = stmt.executeQuery( sql);

			while ( rs.next() ) {
				Long timeStamp  = rs.getLong("TIMESTAMP");
				String sql1="SELECT RNDFTAJ.USERID FROM RNDFTAJ WHERE RNDFTAJ.TIMESTAMP = " + timeStamp ;
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

	public static TreeMap< Long, HashSet<Integer>> getWBMFTAJoinUsersOfTimestaps(){
	
	TreeMap< Long, HashSet<Integer>> result = new TreeMap< Long, HashSet<Integer>>();
	Connection c = null;
	Statement stmt = null, stmt1 = null;
	try {		
		Class.forName("org.sqlite.JDBC");
		c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());	      
		c.setAutoCommit(false);
		stmt = c.createStatement();
		stmt1 = c.createStatement();
		
		String sql="SELECT DISTINCT WBMFTAJ.TIMESTAMP FROM WBMFTAJ ";
		ResultSet rs = stmt.executeQuery( sql);

		while ( rs.next() ) {
			Long timeStamp  = rs.getLong("TIMESTAMP");
			String sql1="SELECT WBMFTAJ.USERID FROM WBMFTAJ WHERE WBMFTAJ.TIMESTAMP = " + timeStamp ;
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
	
	
	public static void main(String[] args){
		
		String srcDB = Config.INSTANCE.getDatasetDb();
		String desDB = srcDB.split("\\.")[0]+"_2.db" ;
		Config.INSTANCE.setDatasetDb(desDB);
//		ResultAnalyser.analysisExperimentJaccard();
//		System.out.println ("  R join"); 
//		HashMap<Long,Integer> RError=computeErrors (getOracleUsersOfTimestaps() , getRJoinUsersOfTimestaps() );
//		System.out.println ("  DW join ");
//		HashMap<Long,Integer> DWError=computeErrors (getOracleUsersOfTimestaps() , getDWJoinUsersOfTimestaps() );
		HashMap<Long,Double> SpError= computeErrorsJaccardIndex  (getOracleUsersOfTimestaps() , getPrefectSJoinUsersOfTimestaps() );

	}

	
}
