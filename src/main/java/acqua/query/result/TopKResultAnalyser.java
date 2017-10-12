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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeMap;

import acqua.config.Config;

public class TopKResultAnalyser {

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
			
			createtable (stmt ,  "OTKJ");
			createtable (stmt ,  "OMTKJ");
			createtable (stmt ,  "WSTJ");
			createtable (stmt ,  "LRUJ");
			createtable (stmt ,  "RNDJ");
			createtable (stmt ,  "WBMJ");
			
			
			putOutputInDatabase( stmt ,"joinOutput/TopKOracleJoinOperatorOutput.txt" , "OTKJ");
			putOutputInDatabase( stmt ,"joinOutput/TopKMtknOracleJoinOperatorOutput.txt" , "OMTKJ");
			putOutputInDatabase( stmt ,"joinOutput/WSTJoinOperatorOutput.txt" , "WSTJ");
			putOutputInDatabase( stmt ,"joinOutput/RNDJoinOperatorOutput.txt" , "RNDJ");
			//putOutputInDatabase( stmt ,"joinOutput/LRUJoinOperatorOutput.txt" , "LRUJ");
			//putOutputInDatabase( stmt ,"joinOutput/WBMJoinOperatorOutput.txt" , "WBMJ");			
			
			
			stmt.close();
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
					" `TIMESTAMP`        BIGINT NOT NULL, "+
					" `SCORE`            REAL    NOT NULL, " + 
					" `RANK`             INT    NOT NULL); " + 
					" CREATE INDEX `" + table + "timeIndex` ON `" + table + "` (`TIMESTAMP` ASC);"; 
			stmt.executeUpdate(sql);
			//System.out.println(sql);
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
				sql = "INSERT INTO " + table + " (USERID,MENTIONCOUNT,FOLLOWERCOUNT,TIMESTAMP,SCORE,RANK) " +
						"VALUES ("+userInfo[0]+","+userInfo[1]+","+userInfo[2]+","+userInfo[3]+","+userInfo[4]+","+userInfo[5]+")"; 
				//System.out.println(sql);
				stmt.executeUpdate(sql);
			}
			br.close();
		} catch (Exception e) {e.printStackTrace(); }
		
		
	}
	
	public static void analysisExperimentNDCG(){
		
		try{
			insertResultToDB();
			BufferedWriter bw=new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare.csv")));
			TreeMap<Long,Integer> oracleCount=computeOJoin();

			//use nDCG to compute errors
			HashMap<Long,Double> OMTKError=computeErrorsNDCG(getResultsOfTimestaps("OTKJ") , getResultsOfTimestaps("OMTKJ") );
			HashMap<Long,Double> WSTError=computeErrorsNDCG(getResultsOfTimestaps("OTKJ") , getResultsOfTimestaps("WSTJ") );
			HashMap<Long,Double> RNDError=computeErrorsNDCG (getResultsOfTimestaps("OTKJ") , getResultsOfTimestaps("RNDJ") );
			HashMap<Long,Double> LRUError=computeErrorsNDCG (getResultsOfTimestaps("OTKJ") , getResultsOfTimestaps("LRUJ") );			
			HashMap<Long,Double> WBMError=computeErrorsNDCG(getResultsOfTimestaps("OTKJ") , getResultsOfTimestaps("WBMJ") );

			
			Iterator<Long> itO = oracleCount.keySet().iterator();
			bw.write("timestampe,Oracle,Oracle MTKN,WST,RND,WBM,LRU\n");
			Double cOC=0.0, comtke =0.0, cwste=0.0 ,crnde=0.0,cwbme=0.0, clrue=0.0 ;
			while(itO.hasNext()){
				
				long nextTime = itO.next();
				Integer OC=oracleCount.get(nextTime);
				Double omtke=OMTKError.get(nextTime);
				Double wste=WSTError.get(nextTime);
				Double rnde=RNDError.get(nextTime);
				Double wbme=WBMError.get(nextTime);
				Double lrue=LRUError.get(nextTime);

				//cumulative error
				cOC=cOC + OC ;
				comtke = comtke + (omtke = omtke==null?0:omtke) ;
				cwste= cwste + (wste = wste==null?0:wste) ;
				crnde= crnde + (rnde = rnde==null?0:rnde) ;
				cwbme= cwbme + (wbme= wbme==null?0:wbme) ;
				clrue= clrue + (lrue= lrue==null?0:lrue) ;
			
				bw.write(nextTime+","+String.format("%.5f",cOC)+
						","+ String.format("%.5f",(comtke==null?0:comtke))+ 
						","+ String.format("%.5f",(cwste==null?0:cwste))+ 
						","+ String.format("%.5f",(crnde==null?0:crnde))+
						","+ String.format("%.5f",(cwbme==null?0:cwbme)) +
						","+ String.format("%.5f",(clrue==null?0:clrue)) + "\n");
				
			}
			bw.flush();
			bw.close();
		}catch(Exception e){e.printStackTrace();}
		
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
			String sql="SELECT otkj.TIMESTAMP as TIMESTAMP, count(OTKJ.USERID) as windowcount FROM OTKJ  group by OTKJ.TIMESTAMP order by OTKJ.TIMESTAMP ASC";      //System.out.println(sql);
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
	}	
	
	public static TreeMap< Long, HashMap<Long ,String>> getResultsOfTimestaps(String table){
		
		TreeMap< Long, HashMap<Long,String>> result = new TreeMap<Long, HashMap<Long,String>>();
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
				String sql1="SELECT USERID, SCORE, RANK FROM " + table + " WHERE " + table + ".TIMESTAMP = " + timeStamp + " ORDER BY RANK ASC" ;
				//System.out.println(sql1);

				ResultSet rs1 = stmt1.executeQuery( sql1);
				HashMap<Long , String> resultSet = new HashMap<Long , String>() ;
				
				while ( rs1.next() ) {
					Long userID  = rs1.getLong("USERID");
					Double score  = rs1.getDouble("SCORE");
					Integer order  = rs1.getInt("RANK");
					resultSet.put(userID , score+","+order);
				}
				result.put(timeStamp,resultSet);
				rs1.close();
				stmt1.close();
			}
			rs.close();
			stmt.close();
			c.close();
		}catch(Exception e){e.printStackTrace();}
		return result;
	}

	public static HashMap<Long,Double> computeErrorsNDCG (TreeMap< Long, HashMap<Long ,String>> original , TreeMap< Long, HashMap<Long ,String>> replica){
	
		HashMap<Long,Double> result=new HashMap<Long, Double>();
		
		Iterator<Long> timeIt= original.keySet().iterator();
		while(timeIt.hasNext()){
			
			Double DCG = 0.0 , IDCG = 0.0 , NDCG = 0.0;
			long timestamp = timeIt.next();
			
			// find same timestamps in original and replica
			HashMap<Long ,String> originalResults = original.get(timestamp);
			HashMap<Long ,String> replicaResults = new HashMap<Long ,String> ();
			if (replica.containsKey(timestamp)) {
				replicaResults = replica.get(timestamp);
			}else { 
				continue; 
			}
			
			//HashSet<String> rerankOriginalResults = rerankOriginalResults(originalResults , replicaResults);
			HashMap<Long ,String> newOriginalResults = setOriginalRelevancy(originalResults);
			HashMap<Long ,String> newReplicaResults = setReplicaRelevancy(replicaResults, originalResults);
			DCG = computeDCG(newReplicaResults);
			IDCG = computeDCG(newOriginalResults);
			//DCG = computeDCG(replicaResults);
			//IDCG = computeDCG(originalResults);
			NDCG = DCG /IDCG;
			
			result.put(timestamp, (1d-NDCG));
			
		}
		return result;
	
	}

	private static Double computeDCG(HashMap<Long ,String> list) {
		
		Double result = 0.0;
		
		Iterator<Long> it= list.keySet().iterator();
		while(it.hasNext()){
			long id =it.next();
			String temp = list.get(id);
			String [] tempSplit = temp.split(",");
			Double score = Double.valueOf(tempSplit[0]);
			Integer rank = Integer.valueOf(tempSplit[1]);

			result +=  ( Math.pow(2, score) - 1 ) / ( Math.log( rank + 1) / Math.log(2) );
			
		}
		return result;
	}

	private static HashSet<String> rerankOriginalResults(HashSet<String> list , HashSet<String> replica) {
		
		HashSet<String> result = new HashSet<String>();
		TreeMap <Integer , String> orderedList = new TreeMap<Integer , String>();
		
		Iterator<String> it= list.iterator();
		while(it.hasNext()){
			String temp =it.next();
			String [] tempSplit = temp.split(",");
			Integer userId = Integer.valueOf(tempSplit[0]);
			Integer rank = Integer.valueOf(tempSplit[2]);
			
			Iterator<String> itrep= replica.iterator();
			while(itrep.hasNext()){
				String temprep =itrep.next();
				String [] temprepSplit = temprep.split(",");
				Integer userIdrep = Integer.valueOf(temprepSplit[0]);
				if (userIdrep.intValue() == userId.intValue()){
						orderedList.put(rank, temp);
				}
			}

		}
		
		Iterator<Integer> ordIt= orderedList.keySet().iterator();
		int newrank = 1;
		while(ordIt.hasNext() && newrank <= Config.INSTANCE.getK()){
			
			Integer oldRank =ordIt.next();
			String temp = orderedList.get(oldRank);
			String [] tempSplit = temp.split(",");
			Integer userId = Integer.valueOf(tempSplit[0]);
			Double score = Double.valueOf(tempSplit[1]);
			Integer rank = Integer.valueOf(tempSplit[2]);
			
			result.add(userId + "," + score + "," + rank);        //newrank removed
			newrank ++;
			
		}
		return result;
	}

	private static HashMap<Long ,String> setOriginalRelevancy(HashMap<Long ,String> original){
		
		HashMap<Long ,String> result = new HashMap<Long ,String>();
		int originalRelevancy = 3;
		
		Iterator<Long> it= original.keySet().iterator();
		while(it.hasNext()){
			long id =it.next();
			String temp = original.get(id);
			String [] tempSplit = temp.split(",");
			Integer rank = Integer.valueOf(tempSplit[1]);
			result.put(id , originalRelevancy + "," + rank);
		}
		return result;
	}
	
	private static HashMap<Long ,String> setReplicaRelevancy(HashMap<Long ,String> replica , HashMap<Long ,String> original){
		
		HashMap<Long ,String> result = new HashMap<Long ,String>();
		int originalRelevancy = 3 ,replicaRelevancy = 1;
		
		Iterator<Long> it= replica.keySet().iterator();
		while(it.hasNext()){
			long id =it.next();
			String temp = replica.get(id);
			String [] tempSplit = temp.split(",");
			Integer rank = Integer.valueOf(tempSplit[1]);
			if (original.containsKey(id)){
				result.put(id , originalRelevancy + "," + rank);
			}else{
				result.put(id , replicaRelevancy + "," + rank);	
			}
		}
		return result;
	}
	
	public static void main(String[] args){
	
		HashMap<Long ,String> original = new HashMap<Long ,String>();
		HashMap<Long ,String> replica = new HashMap<Long ,String>();
		Double DCG = 0.0 , IDCG = 0.0 , NDCG = 0.0;

		
		original.put(101l, "0.9,1");
		original.put(102l, "0.8,2");
		original.put(103l, "0.7,3");
		original.put(104l, "0.6,4");

		replica.put(101l, "0.9,1");
		replica.put(105l, "0.8,2");
		replica.put(103l, "0.7,3");
		replica.put(107l, "0.5,4");

		HashMap<Long ,String> newOriginalResults = setOriginalRelevancy(original);
		HashMap<Long ,String> newReplicaResults = setReplicaRelevancy(replica, original);
		DCG = computeDCG(newReplicaResults);
		IDCG = computeDCG(newOriginalResults);
		
		NDCG = DCG /IDCG;
	}
}
