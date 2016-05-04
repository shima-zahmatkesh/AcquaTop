package acqua.query.result;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
			System.out.println("create tables");
			stmt = c.createStatement();
			
			createtable (stmt ,  "OJ");
			createtable (stmt ,  "WSTJ");
			createtable (stmt ,  "LRUJ");
			createtable (stmt ,  "RNDJ");
			createtable (stmt ,  "WBMJ");
			createtable (stmt ,  "FJ");
			createtable (stmt ,  "LRUFJ");
			createtable (stmt ,  "RNDFJ");
			createtable (stmt ,  "WBMFJ");
			createtable (stmt ,  "LRUFTAJ");
			createtable (stmt ,  "RNDFTAJ");
			createtable (stmt ,  "WBMFTAJ");
			createtable (stmt ,  "LRUFSAJ");
			createtable (stmt ,  "RNDFSAJ");
			createtable (stmt ,  "WBMFSAJ");
			
			putOutputInDatabase( stmt ,"joinOutput/OracleJoinOperatorOutput.txt" , "OJ");
			putOutputInDatabase( stmt ,"joinOutput/WSTJoinOperatorOutput.txt" , "WSTJ");
			putOutputInDatabase( stmt ,"joinOutput/RNDJoinOperatorOutput.txt" , "RNDJ");
			putOutputInDatabase( stmt ,"joinOutput/LRUJoinOperatorOutput.txt" , "LRUJ");
			putOutputInDatabase( stmt ,"joinOutput/WBMJoinOperatorOutput.txt" , "WBMJ");
			putOutputInDatabase( stmt ,"joinOutput/FilterJoinOperatorOutput.txt" , "FJ");
			putOutputInDatabase( stmt ,"joinOutput/LRUFOperatorOutput.txt" , "LRUFJ");
			putOutputInDatabase( stmt ,"joinOutput/RNDFOperatorOutput.txt" , "RNDFJ");
			putOutputInDatabase( stmt ,"joinOutput/WBMFOperatorOutput.txt" , "WBMFJ");
			putOutputInDatabase( stmt ,"joinOutput/LRUFTAOperatorOutput.txt" , "LRUFTAJ");
			putOutputInDatabase( stmt ,"joinOutput/RNDFTAOperatorOutput.txt" , "RNDFTAJ");
			putOutputInDatabase( stmt ,"joinOutput/WBMFTAOperatorOutput.txt" , "WBMFTAJ");
			putOutputInDatabase( stmt ,"joinOutput/LRUFSAOperatorOutput.txt" , "LRUFSAJ");
			putOutputInDatabase( stmt ,"joinOutput/RNDFSAOperatorOutput.txt" , "RNDFSAJ");
			putOutputInDatabase( stmt ,"joinOutput/WBMFSAOperatorOutput.txt" , "WBMFSAJ");

			stmt.close();
			//c.commit();
			c.close();

		}catch(Exception e){e.printStackTrace();}
	}

	public static void createtable (Statement stmt , String table){
		
		try {
			stmt.executeUpdate(" DROP TABLE IF EXISTS " + table + " ;");
			String sql = "CREATE TABLE  `" + table + "` ( " +
					" `USERID`           BIGINT    NOT NULL, " + 
					" `MENTIONCOUNT`     INT    NOT NULL, " + 
					" `FOLLOWERCOUNT`    INT    NOT NULL, " + 
					" `TIMESTAMP`        BIGINT NOT NULL); "+
					" CREATE INDEX `" + table + "timeIndex` ON `" + table + "` (`TIMESTAMP` ASC);"; 
			stmt.executeUpdate(sql);
			
		} catch (SQLException e) { e.printStackTrace();}
	}
	
	public static void putOutputInDatabase (Statement stmt , String outputPath , String table){
		
		InputStream    fis;
		BufferedReader br;
		String line = null;
		String sql = null;
		try {
			fis = new FileInputStream(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder() + outputPath);
			br = new BufferedReader(new InputStreamReader(fis));
			line=null;
			while((line=br.readLine())!=null){
				
				String[] userInfo = line.split(" ");	
				sql = "INSERT INTO " + table + " (USERID,MENTIONCOUNT,FOLLOWERCOUNT,TIMESTAMP) " +
						"VALUES ("+userInfo[0]+","+userInfo[1]+","+userInfo[2]+","+userInfo[3]+")"; 
				//System.out.println(sql);
				stmt.executeUpdate(sql);
			}
			br.close();
		} catch (Exception e) {e.printStackTrace(); }
		
		
	}
	
	public static void analysisExperimentJaccard(){
		
		try{
			insertResultToDB();
			BufferedWriter bw=new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare.csv")));
			TreeMap<Long,Integer> oracleCount=computeOJoin();

			//use Jaccard Index for computing errors
			HashMap<Long,Double> WBMError=computeErrorsJaccardIndex(getUsersOfTimestaps("OJ") , getUsersOfTimestaps("WBMJ") );
			HashMap<Long,Double> RNDError=computeErrorsJaccardIndex (getUsersOfTimestaps("OJ") , getUsersOfTimestaps("RNDJ") );
			HashMap<Long,Double> WSTError=computeErrorsJaccardIndex (getUsersOfTimestaps("OJ") , getUsersOfTimestaps("WSTJ") );
			HashMap<Long,Double> LRUError=computeErrorsJaccardIndex (getUsersOfTimestaps("OJ") , getUsersOfTimestaps("LRUJ") );			
			HashMap<Long,Double> FError=computeErrorsJaccardIndex (getUsersOfTimestaps("OJ") , getUsersOfTimestaps("FJ") );
			
			HashMap<Long,Double> LRUFError=computeErrorsJaccardIndex(getUsersOfTimestaps("OJ") , getUsersOfTimestaps("LRUFJ") );
			HashMap<Long,Double> RNDFError=computeErrorsJaccardIndex(getUsersOfTimestaps("OJ") , getUsersOfTimestaps("RNDFJ") );
			HashMap<Long,Double> WBMFError=computeErrorsJaccardIndex(getUsersOfTimestaps("OJ"), getUsersOfTimestaps("WBMFJ") );
			
		
			Iterator<Long> itO = oracleCount.keySet().iterator();
			bw.write("timestampe,Oracle,WST,RND,WBM,LRU,Filter,LRU.F,RND.F,WBM.F\n");

			Double cOC=0.0, cwste=0.0, crnde=0.0,cwbme=0.0, clrue=0.0, cfe=0.0 ,clrufe =0.0 ,crndfe =0.0 ,cwbmfe =0.0;
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
				
				
				
				bw.write(nextTime+","+String.format("%.2f",cOC)+
						","+ String.format("%.2f",(cwste==null?0:cwste))+ 
						","+ String.format("%.2f",(crnde==null?0:crnde))+
						","+ String.format("%.2f",(cwbme==null?0:cwbme)) +
						","+ String.format("%.2f",(clrue==null?0:clrue)) +
						","+ String.format("%.2f",(cfe==null?0:cfe))+
						","+ String.format("%.2f",(clrufe==null?0:clrufe))+
						","+ String.format("%.2f",(crndfe==null?0:crndfe))+
						","+ String.format("%.2f",(cwbmfe==null?0:cwbmfe))+"\n");

				
			}
			bw.flush();
			bw.close();
		}catch(Exception e){e.printStackTrace();}
		
	}

	public static void analysisMultipleExperimentsJaccard(String index ){
	
		try{
			
			BufferedWriter bw=new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compareMultipleExperiments.csv")));

			bw.write("timestampe,Oracle Min,WST Min,RND Min,WBM Min,LRU Min,Filter Min,LRU.F Min,RND.F Min,WBM.F Min,LRU.F.SA Min,RND.F.SA Min,WBM.F.SA Min,"
							  + "Oracle Max,WST Min,RND Max,WBM Max,LRU Max,Filter Max,LRU.F Max,RND.F Max,WBM.F Max,LRU.F.SA Max,RND.F.SA Max,WBM.F.SA Max,"
							  + "Oracle Avg,WST Avg,RND Avg,WBM Avg,LRU Avg,Filter Avg,LRU.F Avg,RND.F Avg,WBM.F Avg,LRU.F.SA Avg,RND.F.SA Avg,WBM.F.SA Avg\n");
			
			//Min variables
			Double OCMin = Double.MAX_VALUE, wsteMin = Double.MAX_VALUE, rndeMin = Double.MAX_VALUE, wbmeMin = Double.MAX_VALUE, lrueMin = Double.MAX_VALUE; 
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
					Double lruftae = Double.parseDouble(lineSplit[13]);
					Double rndftae = Double.parseDouble(lineSplit[14]);
					Double wbmftae = Double.parseDouble(lineSplit[15]);
				
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
					
					br.close();
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
	
				OCMin = Double.MAX_VALUE; wsteMin = Double.MAX_VALUE; rndeMin = Double.MAX_VALUE; wbmeMin = Double.MAX_VALUE; lrueMin = Double.MAX_VALUE;  
				feMin = Double.MAX_VALUE;lrufeMin =Double.MAX_VALUE ;rndfeMin =Double.MAX_VALUE ;wbmfeMin =Double.MAX_VALUE;lruftaeMin =Double.MAX_VALUE ;rndftaeMin =Double.MAX_VALUE ;wbmftaeMin =Double.MAX_VALUE;
				OCMax = 0.0; wsteMax = 0.0; rndeMax = 0.0; wbmeMax = 0.0; lrueMax = 0.0; feMax = 0.0; lrufeMax =0.0; rndfeMax =0.0; wbmfeMax =0.0;lruftaeMax =0.0; rndftaeMax =0.0; wbmftaeMax =0.0;
				OCAvg = 0.0; wsteAvg = 0.0; rndeAvg = 0.0; wbmeAvg = 0.0; lrueAvg = 0.0; feAvg = 0.0; lrufeAvg =0.0; rndfeAvg =0.0; wbmfeAvg =0.0;lruftaeAvg =0.0; rndftaeAvg =0.0; wbmftaeAvg =0.0;
				OCSum = 0.0; wsteSum = 0.0; rndeSum = 0.0; wbmeSum = 0.0; lrueSum = 0.0; feSum = 0.0; lrufeSum =0.0; rndfeSum =0.0; wbmfeSum =0.0;lruftaeSum =0.0; rndftaeSum =0.0; wbmftaeSum =0.0;
			
			}
			bw.close();
			
		}catch(Exception e){e.printStackTrace();}
		
	}
	
	public static TreeMap< Long, HashSet<Integer>> getUsersOfTimestaps(String table){
		
		TreeMap< Long, HashSet<Integer>> result = new TreeMap< Long, HashSet<Integer>>();
		Connection c = null;
		Statement stmt = null, stmt1 = null;
		try {		
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());	      
			c.setAutoCommit(false);
			stmt = c.createStatement();
			stmt1 = c.createStatement();
			
			String sql="SELECT DISTINCT " + table + ".TIMESTAMP FROM " + table ;
			ResultSet rs = stmt.executeQuery( sql);

			while ( rs.next() ) {
				Long timeStamp  = rs.getLong("TIMESTAMP");
				String sql1="SELECT " + table + ".USERID FROM " + table + " WHERE " + table + ".TIMESTAMP = " + timeStamp ;
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
	
	public static void generateOutputForBoxploting(String [] index){
		

		try{
			BufferedWriter bw=new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compareMultipleExperiments.csv")));
			int minLineNum = 75;

			// find minimum line between all files
//			for (int i=0 ; i < index.length ; i++){
//			
//				for ( int db = 1 ; db<= Config.INSTANCE.getDatabaseNumber() ; db++){
//					
//					LineNumberReader  lnr = new LineNumberReader(new FileReader(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compareMultipleAlpha_"+ db +"_" + index[i] + ".csv")));
//					lnr.skip(Long.MAX_VALUE);
//					if ( lnr.getLineNumber() < minLineNum){
//						minLineNum = lnr.getLineNumber();
//					}
//					System.out.println(minLineNum);
//					lnr.close(); 
//				}
//			}
			
			String lineToWrite = "", firstLineToWrite = ""; String line ="";
			
			for ( int db = 1 ; db<= Config.INSTANCE.getDatabaseNumber() ; db++){

				lineToWrite = " ," + db + ", ,";
				for (int i=0 ; i < index.length ; i++){
					
					BufferedReader br=new BufferedReader(new FileReader(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compareMultipleAlpha_"+ db +"_" + index[i] + ".csv")));
				
					if ( db == 1 && i == 0){
						String firstLine = br.readLine();
						bw.write( firstLine  + "\n");
					}
					
					for( int k =0; k< minLineNum; k++){
						line = br.readLine();
					}
					//System.out.println("line of db"+db +"_"+ index[i] + " = " + line);
					bw.write(line + ", ,");
					
					br.close();

				}
				
				bw.write("\n");
			}
			//br.close();
			bw.close();
		}catch(Exception e){e.printStackTrace();}
	}
	
	public static void analysisExperimentScoringAlgorithm(){
		
		try{
			insertResultToDB();
			BufferedWriter bw=new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare_alpha.csv")));
			TreeMap<Long,Integer> oracleCount=computeOJoin();

			//use Jaccard Index for computing errors

			
			HashMap<Long,Double> LRUFSAError=computeErrorsJaccardIndex(getUsersOfTimestaps("OJ") , getUsersOfTimestaps("LRUFSAJ") );
			HashMap<Long,Double> RNDFSAError=computeErrorsJaccardIndex(getUsersOfTimestaps("OJ") , getUsersOfTimestaps("RNDFSAJ") );
			HashMap<Long,Double> WBMFSAError=computeErrorsJaccardIndex(getUsersOfTimestaps("OJ") , getUsersOfTimestaps("WBMFSAJ") );
			
			Iterator<Long> itO = oracleCount.keySet().iterator();
			bw.write("timestampe,Oracle,LRU.F.SA,RND.F.SA,WBM.F.SA\n");

			Double cOC=0.0, clrufsae =0.0 ,crndfsae =0.0 ,cwbmfsae =0.0;
			while(itO.hasNext()){
				
				long nextTime = itO.next();
				Integer OC=oracleCount.get(nextTime);
				
				Double lrufsae = LRUFSAError.get(nextTime);
				Double rndfsae = RNDFSAError.get(nextTime);
				Double wbmfsae = WBMFSAError.get(nextTime);

				//cumulative error
				cOC=cOC + OC ; 
				clrufsae = clrufsae + (lrufsae= lrufsae==null?0:lrufsae) ;
				crndfsae = crndfsae + (rndfsae= rndfsae==null?0:rndfsae) ;
				cwbmfsae = cwbmfsae + (wbmfsae= wbmfsae==null?0:wbmfsae) ;
				
				
				bw.write(nextTime+","+String.format("%.2f",cOC)+
						","+ String.format("%.2f",(clrufsae==null?0:clrufsae))+
						","+ String.format("%.2f",(crndfsae==null?0:crndfsae))+
						","+ String.format("%.2f",(cwbmfsae==null?0:cwbmfsae))+"\n");

				
			}
			bw.flush();
			bw.close();
		}catch(Exception e){e.printStackTrace();}
		
	}

	public static void analysisExperimentScoringAlgorithmMerge(String [] alpha , String percentage , String db){
		
		try {
			
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compareMultipleAlpha_"+ db + "_" + percentage + ".csv")));
			String writeLine = "timestampe,Oracle,WST,RND,WBM,LRU,Filter,LRU.F,RND.F,WBM.F" ;
			for ( int i = 0 ; i < alpha.length ; i++){
				writeLine = writeLine.concat (",LRU.F.SA." + alpha[i] + ",RND.F.SA." + alpha[i] + ",WBM.F.SA." + alpha[i]  ) ;
			}
			writeLine = writeLine.concat ("\n");
			bw.write(writeLine);
			
			writeLine = "";
			String line ="";
			
			for ( int e = 0 ; e <= Config.INSTANCE.getExperimentIterationNumber() ; e++){
				
				BufferedReader br1=new BufferedReader(new FileReader(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare.csv")));

				for( int k =0;k<=e; k++){
					line = br1.readLine();
				}
				if (e==0){
					continue;
				}
				if (line == null){
					line = " , ,0,0,0,0,0,0,0,0";
				}
				writeLine = line;
				for ( int i = 0 ; i < alpha.length ; i++){
					
					BufferedReader br2=new BufferedReader(new FileReader(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare_alpha_" + db + "_"+ percentage + "_" + alpha[i] +".csv")));
					
					for( int k =0;k<=e; k++){
						line = br2.readLine();
					}
					if (e==0){
						continue;
					}
					if (line == null){
						line = "0,0,0,0,0";
					}
					String [] lineSplit = line.split(",| ");
					writeLine = writeLine.concat("," + lineSplit[2] + "," + lineSplit[3] + "," + lineSplit[4] );
					
				}
				writeLine = writeLine.concat ("\n");
				bw.write(writeLine);
			}
			bw.close();
		
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
				
	}
	
	public static void analysisMultipleExperimentsScoringAlgorithm(String [] alpha ,String index ){
		
		try{
			
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compareMultipleAlpha_" + index + ".csv")));
			String writeLine = "timestampe,WST Min,RND Min,WBM Min,LRU Min,Filter Min,LRU.F Min,RND.F Min,WBM.F Min" ;
			for ( int i = 0 ; i < alpha.length ; i++){
				writeLine = writeLine.concat (",LRU.F.SA." + alpha[i] + " Min,RND.F.SA." + alpha[i] + "Min ,WBM.F.SA." + alpha[i]+ " Min"  ) ;
			}
			writeLine = writeLine.concat ("WST Min,RND Max,WBM Max,LRU Max,Filter Max,LRU.F Max,RND.F Max,WBM.F Max");
			
			for ( int i = 0 ; i < alpha.length ; i++){
				writeLine = writeLine.concat (",LRU.F.SA." + alpha[i] + " Max,RND.F.SA." + alpha[i] + "Max ,WBM.F.SA." + alpha[i]+ " Max"  ) ;
			}
			writeLine = writeLine.concat ("WST Avg,RND Avg,WBM Avg,LRU Avg,Filter Avg,LRU.F Avg,RND.F Avg,WBM.F Avg");
			for ( int i = 0 ; i < alpha.length ; i++){
				writeLine = writeLine.concat (",LRU.F.SA." + alpha[i] + " Avg,RND.F.SA." + alpha[i] + "Avg ,WBM.F.SA." + alpha[i]+ " Avg"  ) ;
			}
			writeLine = writeLine.concat ("\n");
			bw.write(writeLine);

			Double [] err = new Double[1 + 3 * (8 + 3* alpha.length)] ;
			Double [] Min = new Double[1 + 3 * (8 + 3* alpha.length)] ; 
			Double [] Max = new Double[1 + 3 * (8 + 3* alpha.length)] ;
			Double [] Avg = new Double[1 + 3 * (8 + 3* alpha.length)] ;
			Double [] Sum = new Double[1 + 3 * (8 + 3* alpha.length)] ;
			long nextTime = 0 ;
			int splitNum = 0;
			String line = "";
			for ( int e = 0 ; e <=75  ; e++){   //Config.INSTANCE.getExperimentIterationNumber() ; e++){
				
				int nullLine = 0;
				for ( int db = 1 ; db<= Config.INSTANCE.getDatabaseNumber() ; db++){
					
					BufferedReader br2=new BufferedReader(new FileReader(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compareMultipleAlpha_"+ db +"_" + index + ".csv")));
					
					for( int k =0;k<=e; k++){
						line = br2.readLine();
					}
					if (e==0){
						continue;
					}
					if (line == null){
						nullLine ++;
						System.out.println("iteration = " + e + "   database = " + db  +"   null line = "+ nullLine);
						continue ;
					}
					String [] lineSplit = line.split(",| ");
					splitNum = lineSplit.length;
					System.out.println(splitNum);
					for ( int i = 0 ; i < splitNum ; i++ ){
						Min[i] = Double.MAX_VALUE;
						Max[i] = 0.0;
						Avg[i] = 0.0;
						Sum[i] = 0.0;
					}
					for ( int i = 0 ; i < splitNum ; i++ ){
						if ( i == 0){
							nextTime = Long.parseLong(lineSplit[i]);
						}
						else if (i == 1){
							continue;
						}
						else{
							err[i] = Double.parseDouble(lineSplit[i]);
							if (err[i] < Min[i] )  Min[i] = err[i];
							if (err[i] > Max [i])  Max [i]= err[i];
							Sum[i] +=err[i];
						}
					}
					br2.close();	
				}
			
				int totalNum = Config.INSTANCE.getDatabaseNumber() - nullLine;
				writeLine = "";
				for ( int i = 0 ; i < splitNum ; i++ ){
				Avg [i] = Sum[i] / totalNum;
				}
				
				writeLine = String.valueOf (nextTime)  ;
				for ( int i = 0 ; i < splitNum ; i++ )
					writeLine = writeLine.concat( "," + Min[i]);
				for ( int i = 0 ; i < splitNum ; i++ )
					writeLine = writeLine.concat( "," + Max[i]);
				for ( int i = 0 ; i < splitNum ; i++ )
					writeLine = writeLine.concat( "," + Avg[i]);
				
				writeLine = writeLine.concat ("\n");
				bw.write(writeLine);
	
				for ( int i = 0 ; i < splitNum ; i++ ){
					Min[i] = Double.MAX_VALUE;
					Max[i] = Double.MIN_VALUE;
					Avg[i] = 0.0;
					Sum[i] = 0.0;
				}
			}
			bw.close();
			
		}catch(Exception e){e.printStackTrace();}
		
	}
	
	public static void main(String[] args){
		
		
		String [] index = { "10", "20", "25", "30" , "40" , "50" , "60" ,"70" };
		generateOutputForBoxploting(index);


	}

	
}
