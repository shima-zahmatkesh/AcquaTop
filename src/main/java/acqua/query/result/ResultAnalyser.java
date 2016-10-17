package acqua.query.result;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.commons.lang.ArrayUtils;

import com.sun.source.tree.ContinueTree;

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
			createtable (stmt ,  "WBMFSAOJ");
			//createtable (stmt ,  "FCJ");
			//createtable (stmt ,  "LRUCJ");
			//createtable (stmt ,  "WBMCJ");
			//createtable (stmt ,  "FPVJ");
			
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
			putOutputInDatabase( stmt ,"joinOutput/WBMFSAOneListOperatorOutput.txt" , "WBMFSAOJ");
			//putOutputInDatabase( stmt ,"joinOutput/FilterCOperatorOutput.txt" , "FCJ");
			//putOutputInDatabase( stmt ,"joinOutput/LRUCOperatorOutput.txt" , "LRUCJ");
			//putOutputInDatabase( stmt ,"joinOutput/WBMCOperatorOutput.txt" , "WBMCJ");
			//putOutputInDatabase( stmt ,"joinOutput/FilterPVOperatorOutput.txt" , "FPVJ");
			
			
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
				//System.out.println(line);
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
			
		
			//HashMap<Long,Double> FCError=computeErrorsJaccardIndex(getUsersOfTimestaps("OJ") , getUsersOfTimestaps("FCJ") );
			//HashMap<Long,Double> LRUCError=computeErrorsJaccardIndex(getUsersOfTimestaps("OJ") , getUsersOfTimestaps("LRUCJ") );
			//HashMap<Long,Double> WBMCError=computeErrorsJaccardIndex(getUsersOfTimestaps("OJ"), getUsersOfTimestaps("WBMCJ") );
			
			//HashMap<Long,Double> FPVError=computeErrorsJaccardIndex(getUsersOfTimestaps("OJ") , getUsersOfTimestaps("FPVJ") );

			
			Iterator<Long> itO = oracleCount.keySet().iterator();
		//	bw.write("timestampe,Oracle,WST,RND,WBM,LRU,Filter,LRU.F,RND.F,WBM.F,FC,LRUC, WBMC ,FPV\n");
			bw.write("timestampe,Oracle,WST,RND,WBM,LRU,Filter,LRU.F,RND.F,WBM.F\n");
			Double cOC=0.0, cwste=0.0, crnde=0.0,cwbme=0.0, clrue=0.0, cfe=0.0 ,clrufe =0.0 ,crndfe =0.0 ,cwbmfe =0.0,clruce =0.0 ,cfce =0.0 ,cwbmce =0.0 , cfpve = 0.0 ;
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
				
				//Double lruce = LRUCError.get(nextTime);
				//Double fce = FCError.get(nextTime);
				//Double wbmce = WBMCError.get(nextTime);
				
				//Double fpve = FPVError.get(nextTime);

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
				
			//	clruce = clruce + (lruce= lruce==null?0:lruce) ;
			//	cfce = cfce + (fce= fce==null?0:fce) ;
			//	cwbmce = cwbmce + (wbmce= wbmce==null?0:wbmce) ;
				
			//	cfpve = cfpve + (fpve= fpve==null?0:fpve) ;
				
				
				bw.write(nextTime+","+String.format("%.2f",cOC)+
						","+ String.format("%.2f",(cwste==null?0:cwste))+ 
						","+ String.format("%.2f",(crnde==null?0:crnde))+
						","+ String.format("%.2f",(cwbme==null?0:cwbme)) +
						","+ String.format("%.2f",(clrue==null?0:clrue)) +
						","+ String.format("%.2f",(cfe==null?0:cfe))+
						","+ String.format("%.2f",(clrufe==null?0:clrufe))+
						","+ String.format("%.2f",(crndfe==null?0:crndfe))+
						","+ String.format("%.2f",(cwbmfe==null?0:cwbmfe))+"\n");
						//","+ String.format("%.2f",(clruce==null?0:clruce))+
						//","+ String.format("%.2f",(cfce==null?0:cfce))+
						//","+ String.format("%.2f",(cwbmce==null?0:cwbmce))+
						//","+ String.format("%.2f",(cfpve==null?0:cfpve))+"\n");
				
			}
			bw.flush();
			bw.close();
		}catch(Exception e){e.printStackTrace();}
		
	}

	public static void analysisMultipleExperimentsJaccard(String index ){
	
		try{
			
			BufferedWriter bw=new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compareMultipleExperiments.csv")));

			bw.write("timestampe,Oracle Min,WST Min,RND Min,WBM Min,LRU Min,Filter Min,LRU.F Min,RND.F Min,WBM.F Min,LRU.F+ Min,RND.F+ Min,WBM.F+ Min,"
							  + "Oracle Max,WST Min,RND Max,WBM Max,LRU Max,Filter Max,LRU.F Max,RND.F Max,WBM.F Max,LRU.F+ Max,RND.F+ Max,WBM.F+ Max,"
							  + "Oracle Avg,WST Avg,RND Avg,WBM Avg,LRU Avg,Filter Avg,LRU.F Avg,RND.F Avg,WBM.F Avg,LRU.F+ Avg,RND.F+ Avg,WBM.F+ Avg\n");
			
//			bw.write("timestampe,Oracle Min,WST Min,RND Min,WBM Min,LRU Min,Filter Min,LRU.F Min,RND.F Min,WBM.F Min,F.C Min,LRU.F.C Min,WBM.F.C Min,FPV Min,"
//					  + "Oracle Max,WST Min,RND Max,WBM Max,LRU Max,Filter Max,LRU.F Max,RND.F Max,WBM.F Max,F.C Max,LRU.F.C Max,WBM.F.C Max, FPV Max,"
//					  + "Oracle Avg,WST Avg,RND Avg,WBM Avg,LRU Avg,Filter Avg,LRU.F Avg,RND.F Avg,WBM.F Avg,F.C Avg,LRU.F.C Avg,WBM.F.C Avg, FPV Avg\n");
			
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
					Double lruftae = Double.parseDouble(lineSplit[10]);
					Double rndftae = Double.parseDouble(lineSplit[0]);
					Double wbmftae = Double.parseDouble(lineSplit[0]);
				
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
	
	
	
/////////////////////////////////////////////////   START - FUNCTIONS RELATED TO EXPERIMENTS WITH DIFFERENT VALUE OF ALPHA   /////////////////////////////////////////////////	
	
	public static void analysisExperimentScoringAlgorithm(){
		
		try{
			insertResultToDB();
			BufferedWriter bw=new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare_alpha.csv")));
			TreeMap<Long,Integer> oracleCount=computeOJoin();

			//use Jaccard Index for computing errors

			
			HashMap<Long,Double> LRUFSAError=computeErrorsJaccardIndex(getUsersOfTimestaps("OJ") , getUsersOfTimestaps("LRUFSAJ") );
			HashMap<Long,Double> RNDFSAError=computeErrorsJaccardIndex(getUsersOfTimestaps("OJ") , getUsersOfTimestaps("RNDFSAJ") );
			HashMap<Long,Double> WBMFSAError=computeErrorsJaccardIndex(getUsersOfTimestaps("OJ") , getUsersOfTimestaps("WBMFSAJ") );
			HashMap<Long,Double> WBMFSAOError=computeErrorsJaccardIndex(getUsersOfTimestaps("OJ") , getUsersOfTimestaps("WBMFSAOJ") );
			
			Iterator<Long> itO = oracleCount.keySet().iterator();
			bw.write("timestampe,Oracle,LRU.F+,RND.F+,WBM.F+,WBM.F*\n");

			Double cOC=0.0, clrufsae =0.0 ,crndfsae =0.0 ,cwbmfsae =0.0,cwbmfsaoe =0.0;
			while(itO.hasNext()){
				
				long nextTime = itO.next();
				Integer OC=oracleCount.get(nextTime);
				
				Double lrufsae = LRUFSAError.get(nextTime);
				Double rndfsae = RNDFSAError.get(nextTime);
				Double wbmfsae = WBMFSAError.get(nextTime);
				Double wbmfsaoe = WBMFSAOError.get(nextTime);
				
				//cumulative error
				cOC=cOC + OC ; 
				clrufsae = clrufsae + (lrufsae= lrufsae==null?0:lrufsae) ;
				crndfsae = crndfsae + (rndfsae= rndfsae==null?0:rndfsae) ;
				cwbmfsae = cwbmfsae + (wbmfsae= wbmfsae==null?0:wbmfsae) ;
				cwbmfsaoe = cwbmfsaoe + (wbmfsaoe= wbmfsaoe==null?0:wbmfsaoe) ;
				
				bw.write(nextTime+","+String.format("%.2f",cOC)+
						","+ String.format("%.2f",(clrufsae==null?0:clrufsae))+
						","+ String.format("%.2f",(crndfsae==null?0:crndfsae))+
						","+ String.format("%.2f",(cwbmfsaoe==null?0:cwbmfsaoe))+
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
				writeLine = writeLine.concat (",LRU.F+" + alpha[i] + ",RND.F+" + alpha[i] + ",WBM.F+" + alpha[i] + ",WBM.F*" + alpha[i] ) ;
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
					line = "0,0,0,0,0,0,0,0,0,0";
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
						line = "0,0,0,0,0,0";
					}
					//System.out.println("line =" + line);
					String [] lineSplit = line.split(",| ");
					writeLine = writeLine.concat("," + lineSplit[2] + "," + lineSplit[3] + "," + lineSplit[4] + "," + lineSplit[5]);
					
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
	
	public static void analysisMultipleExperimentsScoringAlgorithm(String [] alpha ,String percentage ){
		
		try{
			
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compareMultipleAlpha_" + percentage + ".csv")));
			String writeLine = "timestampe,WST Min,RND Min,WBM Min,LRU Min,Filter Min,LRU.F Min,RND.F Min,WBM.F Min" ;
			for ( int i = 0 ; i < alpha.length ; i++){
				writeLine = writeLine.concat (",LRU.F+" + alpha[i] + " Min,RND.F+" + alpha[i] + "Min ,WBM.F+" + alpha[i]+ "Min ,WBM.F*" + alpha[i]+ " Min"  ) ;
			}
			writeLine = writeLine.concat (",WST Min,RND Max,WBM Max,LRU Max,Filter Max,LRU.F Max,RND.F Max,WBM.F Max");
			
			for ( int i = 0 ; i < alpha.length ; i++){
				writeLine = writeLine.concat (",LRU.F+" + alpha[i] + " Max,RND.F+" + alpha[i] + "Max ,WBM.F+" + alpha[i]+ "Max ,WBM.F*" + alpha[i]+ " Max"    ) ;
			}
			writeLine = writeLine.concat (",WST Median,RND Median,WBM Median,LRU Median,Filter Median,LRU.F Median,RND.F Median,WBM.F Median");
			for ( int i = 0 ; i < alpha.length ; i++){
				writeLine = writeLine.concat (",LRU.F+" + alpha[i] + " Median,RND.F+" + alpha[i] + "Median ,WBM.F+" + alpha[i]+ "Median ,WBM.F*" + alpha[i]+ " Median"   ) ;
			}
			writeLine = writeLine.concat ("\n");
			bw.write(writeLine);

			Double [] err = new Double[1 + 3 * (8 + 4* alpha.length)] ;
			Double [] Min = new Double[1 + 3 * (8 + 4* alpha.length)] ; 
			Double [] Max = new Double[1 + 3 * (8 + 4* alpha.length)] ;
			Double [] Med = new Double[1 + 3 * (8 + 4* alpha.length)] ;
			Double [] Sum = new Double[1 + 3 * (8 + 4* alpha.length)] ;
			Double [] Avg = new Double[1 + 3 * (8 + 4* alpha.length)] ;
			Double [][] MedTemp = new Double  [1 + 3 * (8 + 4* alpha.length)][10] ;
			Double [][] Medians = new Double  [1 + 3 * (8 + 4* alpha.length)][10] ;
			
			String nextTime = "0" ;
			int splitNum = 0;
			String line = "";
			for ( int e = 0 ; e <=140  ; e++){   //Config.INSTANCE.getExperimentIterationNumber() ; e++){

				int nullLine = 0;
				for ( int db = 1 ; db<= Config.INSTANCE.getDatabaseNumber() ; db++){
					
					BufferedReader br2=new BufferedReader(new FileReader(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compareMultipleAlpha_"+ db +"_" + percentage + ".csv")));
					
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
					for ( int i = 0 ; i < splitNum ; i++ ){
						Min[i] = Double.MAX_VALUE;
						Max[i] = 0.0;
						Avg[i] = 0.0;
						Sum[i] = 0.0;
						Med[i] = 0.0;
						
					}
					for ( int i = 0 ; i < splitNum ; i++ ){
						if ( i == 0){
							nextTime = lineSplit[i];
						}
						else if (i == 1){
							continue;
						}
						else{
							System.out.println("iteration = " + e + "   database = " + db  );
							err[i] = Double.parseDouble(lineSplit[i]);
							if (err[i] < Min[i] )  Min[i] = err[i];
							if (err[i] > Max [i])  Max [i]= err[i];
							Sum[i] +=err[i];
							MedTemp[i][db-1] = err[i];	
						}
					}
					br2.close();
						
				}
				int totalNum = Config.INSTANCE.getDatabaseNumber() - nullLine;
				writeLine = "";
				
				for ( int i = 2 ; i < splitNum ; i++ ){
					Avg [i] = Sum[i] / totalNum;
					Med[i] = getMedian(MedTemp[i]);	
				}
				
				writeLine = nextTime  ;
				for ( int i = 2 ; i < splitNum ; i++ )
					writeLine = writeLine.concat( "," + Min[i]);
				for ( int i = 2 ; i < splitNum ; i++ )
					writeLine = writeLine.concat( "," + Max[i]);
				for ( int i = 2 ; i < splitNum ; i++ )
					writeLine = writeLine.concat( "," + Med[i]);
				
				writeLine = writeLine.concat ("\n");
				bw.write(writeLine);
	
				for ( int i = 0 ; i < splitNum ; i++ ){
					Min[i] = Double.MAX_VALUE;
					Max[i] = Double.MIN_VALUE;
					Avg[i] = 0.0;
					Sum[i] = 0.0;
					Med[i] = 0.0;
				}
			}
			bw.close();
			
		}catch(Exception e){e.printStackTrace();}
		
	}
	
	public static double getMedian (Double[] numArray){
		
		Arrays.sort(numArray);
		int middle = numArray.length/2;
		double medianValue = 0; //declare variable 
		if (numArray.length%2 == 1) 
		    medianValue = numArray[middle];
		else
		   medianValue = (numArray[middle-1] + numArray[middle]) / 2;
		

		return medianValue;
	}
	
	
/////////////////////////////////////////////////   END - FUNCTIONS RELATED TO EXPERIMENTS WITH DIFFERENT VALUE OF ALPHA   ///////////////////////////////////////////////////		

	
	
	
/////////////////////////////////////////////////////    START - FUNCTIONS RELATED TO PARTIAL VIEW EXPERIMENTS      //////////////////////////////////////////////////////////	

public static void analysisPartialViewExperiment(){
		
		try{
			insertResultToDB();
			BufferedWriter bw=new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare.csv")));
			TreeMap<Long,Integer> oracleCount=computeOJoin();

			//use Jaccard Index for computing errors
			
			
			HashMap<Long,Double> FPVError=computeErrorsJaccardIndex(getUsersOfTimestaps("OJ") , getUsersOfTimestaps("FPVJ") );

			
			Iterator<Long> itO = oracleCount.keySet().iterator();
			bw.write("timestampe,FPV\n");

			Double  cfpve = 0.0 ;
			while(itO.hasNext()){
				
				long nextTime = itO.next();
				Double fpve = FPVError.get(nextTime);
				cfpve = cfpve + (fpve= fpve==null?0:fpve) ;
				bw.write(nextTime +","+ String.format("%.2f",(cfpve==null?0:cfpve))+"\n");
				
			}
			bw.flush();
			bw.close();
		}catch(Exception e){e.printStackTrace();}
		
	}


public static void analysisPartialViewExperimentMerge(int [] k , String percentage , int db){
	
	try {
		
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compareMultipleK_"+ db + "_" + percentage + ".csv")));
		String writeLine = "timestampe" ;
		for ( int i = 0 ; i < k.length ; i++){
			writeLine = writeLine.concat ("," + k[i] ) ;
		}
		//writeLine = writeLine.concat ("\n");
		bw.write(writeLine);
		
		writeLine = "";
		String line ="";
		
		for ( int e = 0 ; e <= Config.INSTANCE.getExperimentIterationNumber() ; e++){
			
			
			for ( int i = 0 ; i < k.length ; i++){
				
				BufferedReader br2=new BufferedReader(new FileReader(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare_k_" + db + "_"+ percentage + "_" + k[i] +".csv")));
				
				for( int j =0;j<=e; j++){
					line = br2.readLine();
				}
				if (e==0){
					continue;
				}
				if (line == null){
					line = "0,0";
				}
				//System.out.println("line =" + line + " from    compare_k_" + db + "_"+ percentage + "_" + k[i] +".csv");
				String [] lineSplit = line.split(",| ");
				
				if ( i == 0){
					writeLine = writeLine.concat( lineSplit[0] +"," + lineSplit[1] );
				}
				else{
					writeLine = writeLine.concat("," + lineSplit[1] );
				}
				
			}
			writeLine = writeLine.concat ("\n");
			bw.write(writeLine);
			writeLine = "";
		}
		bw.close();
	
	
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
			
}


/////////////////////////////////////////////////////    END - FUNCTIONS RELATED TO PARTIAL VIEW EXPERIMENTS      //////////////////////////////////////////////////////////	
 


public static void generateDataForPlotingSelectivity( int maxline){
	
	//String [] percentage ={ "10", "20", "25", "30" , "40" , "50" , "60" ,"70" ,"80" , "90" };

	String [] percentage ={ "30", "60"  };
	
			try {
		
				BufferedWriter bw = new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/DataForPloting.csv")));
				String writeLine = "percentage,db,policy,CJD,selectivity\n";
				bw.write(writeLine);
				
				for (int db=1 ; db<= Config.INSTANCE.getDatabaseNumber() ; db++){
					
					for (int p=0 ; p < percentage.length ; p++){
				
						BufferedReader br=new BufferedReader(new FileReader(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compareMultipleAlpha_"+ db +"_" + percentage[p] + ".csv")));
						String line ="" , firstLine = "";
						
						for( int k =0; k <= maxline; k++){
							if ( k == 0)
								firstLine = br.readLine(); 
							else
								line = br.readLine();
						}
						String [] fisrtLineSplit = firstLine.split(",| ");
						String [] lineSplit = line.split(",| ");
						//System.out.println(fisrtLineSplit.length + " line length"+lineSplit.length);
						for ( int i = 0 ; i < fisrtLineSplit.length ; i++){
							
							if ( fisrtLineSplit[i].equals("timestampe") ||  fisrtLineSplit[i].equals("Oracle") ||  fisrtLineSplit[i].equals("WST"))
								continue;
							//if ( i < 10) {
							System.out.println("i = " + i + "p = " + p + " "+ fisrtLineSplit[i]);
							writeLine = percentage[p]+","+db+","+ fisrtLineSplit[i]+","+ lineSplit[i]+","+(100-Integer.valueOf(percentage[p]))+"\n";
							//}else{
							//	System.out.println(i + "db = " + db + "percentage = "+ percentage[p] );
							//	writeLine = percentage[p]+","+db+","+ fisrtLineSplit[i]+","+ lineSplit[i+4]+"\n";
							//}
							bw.write(writeLine);
						}
					}
				}
			
				bw.close();
	
	
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
}

public static void generateDataForPlotingBudget(int maxline){
	
	//String [] budget = { "1", "2", "3" , "4" , "5" , "6" ,"7" };

	String [] budget = { "3",  "5" };

			try {
		
				BufferedWriter bw = new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/DataForPloting.csv")));
				String writeLine = "budget,db,policy,CJD\n";
				bw.write(writeLine);
				
				for (int db=1 ; db<= Config.INSTANCE.getDatabaseNumber() ; db++){
					
					for (int b=0 ; b < budget.length ; b++){
				
						BufferedReader br=new BufferedReader(new FileReader(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compareMultipleAlpha_"+ db +"_" + budget[b] + ".csv")));
						String line ="" , firstLine = "";
						
						for( int k =0; k <= maxline; k++){
							if ( k == 0)
								firstLine = br.readLine(); 
							else
								line = br.readLine();
						}
						String [] fisrtLineSplit = firstLine.split(",| ");
						String [] lineSplit = line.split(",| ");
						//System.out.println(fisrtLineSplit.length + " line length"+lineSplit.length);
						for ( int i = 0 ; i < fisrtLineSplit.length ; i++){
							if ( fisrtLineSplit[i].equals("timestampe") ||  fisrtLineSplit[i].equals("Oracle") ||  fisrtLineSplit[i].equals("WST"))
								continue;
							//if ( i < 10) {
								writeLine = budget[b]+","+db+","+ fisrtLineSplit[i]+","+ lineSplit[i]+"\n";
							//}else{
							//	System.out.println(i + "db = " + db + "percentage = "+ budget[b]);
							//	writeLine = budget[b]+","+db+","+ fisrtLineSplit[i]+","+ lineSplit[i+4]+"\n";
							//}
							bw.write(writeLine);	
						}
					}
				}
			
				bw.close();
	
	
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
}

public static void generateDataForPlotingMultiRun(){
	
	
	
	int maxline = 28;
			try {
		
				BufferedWriter bw = new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/DataForPloting.csv")));
				String writeLine = "iteration,policy,CJD\n";
				bw.write(writeLine);
				writeLine = "";
				BufferedReader br=new BufferedReader(new FileReader(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/test.csv")));
				String line ="" , firstLine = "";
						
				firstLine = br.readLine(); 
				String [] fisrtLineSplit = firstLine.split(",| ");
				
				for ( int k  =  0 ; k <= maxline ; k++){
					line = br.readLine();
					String [] lineSplit = line.split(",| ");
						
					System.out.println(fisrtLineSplit.length + " line length"+lineSplit.length);
					for ( int i = 1 ; i < lineSplit.length ; i++){
						writeLine = writeLine.concat(lineSplit[0]+","+ fisrtLineSplit[i]+","+ lineSplit[i]+"\n" ) ;
					}
					
				}
				bw.write(writeLine);
				bw.close();
	
	
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
}

public static void main(String[] args){
		
		
		//String [] index = { "10", "20", "25", "30" , "40" , "50" , "60" ,"70" };
		//generateOutputForBoxploting(index);
		//Double [] test = { 2.0,3.0,3.0,7.0,8.0,9.0,4.0,5.0,7.0, 10.0};
		//double d = getMedian(test);
		//System.out.println(" median  = " + d);
		generateDataForPlotingBudget(110);
		generateDataForPlotingSelectivity(110);
		//generateDataForPlotingMultiRun();
	}

	
}
