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
			createtable (stmt ,  "WSTTKJ");
			
			putOutputInDatabase( stmt ,"joinOutput/TopKOracleJoinOperatorOutput.txt" , "OTKJ");
			putOutputInDatabase( stmt ,"joinOutput/WSTTopKJoinOperatorOutput.txt" , "WSTTKJ");
			
			
			
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
					" `TIMESTAMP`        BIGINT NOT NULL); "+
					" `SCORE`            REAL    NOT NULL, " + 
					" `ORDER`            INT    NOT NULL, " + 
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
				sql = "INSERT INTO " + table + " (USERID,MENTIONCOUNT,FOLLOWERCOUNT,TIMESTAMP,SCORE,ORDER) " +
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
			HashMap<Long,Double> WSTError=computeErrorsNDCG(getResultsOfTimestaps("OTKJ") , getResultsOfTimestaps("WSTTKJ") );
			
			
			Iterator<Long> itO = oracleCount.keySet().iterator();
			bw.write("timestampe,Oracle,WST\n");
			Double cOC=0.0, cwste=0.0 ;
			while(itO.hasNext()){
				
				long nextTime = itO.next();
				Integer OC=oracleCount.get(nextTime);
				Double wste=WSTError.get(nextTime);

				//cumulative error
				cOC=cOC + OC ; 
				cwste= cwste + (wste = wste==null?0:wste) ;
			
				bw.write(nextTime+","+String.format("%.2f",cOC)+ ","+ String.format("%.2f",(cwste==null?0:cwste))+ "\n");
				
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
	}	
	
	public static TreeMap< Long, HashSet<String>> getResultsOfTimestaps(String table){
		
		TreeMap< Long, HashSet<String>> result = new TreeMap< Long, HashSet<String>>();
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
				String sql1="SELECT USERID,SCORE,ORDER FROM " + table + " WHERE " + table + ".TIMESTAMP = " + timeStamp + "ORDER BY ORDER ASC" ;
				ResultSet rs1 = stmt1.executeQuery( sql1);
				HashSet<String> resultSet = new HashSet<String>() ;
				
				while ( rs1.next() ) {
					Integer userID  = rs1.getInt("USERID");
					Double score  = rs1.getDouble("SCORE");
					Integer order  = rs1.getInt("ORDER");
					resultSet.add(userID+","+score+","+order);
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

	public static HashMap<Long,Double> computeErrorsNDCG (TreeMap< Long, HashSet<String>> original , TreeMap< Long, HashSet<String>> replica){
	
		HashMap<Long,Double> result=new HashMap<Long, Double>();
		
		Iterator<Long> timeIt= original.keySet().iterator();
		while(timeIt.hasNext()){
			
			Double DCG = 0.0 , IDCG = 0.0 , NDCG = 0.0;
			long timestamp = timeIt.next();
			
			// find same timestamps in original and replica
			HashSet<String> originalResults = original.get(timestamp);
			HashSet<String> replicaResults = new HashSet<String>();
			if (replica.containsKey(timestamp)) {
				replicaResults = replica.get(timestamp);
			}else { 
				continue; 
			}
			
			DCG = computeDCG(replicaResults);
			IDCG = computeDCG(originalResults);
			NDCG = DCG /IDCG;
			
			result.put(timestamp, NDCG);
			
		}
		return result;
	
	}

	private static Double computeDCG(HashSet<String> list) {
		
		Double result = 0.0;
		
		Iterator<String> it= list.iterator();
		while(it.hasNext()){
			String temp =it.next();
			String [] tempSplit = temp.split(",");
			Integer userId = Integer.valueOf(tempSplit[0]);
			Double score = Double.valueOf(tempSplit[1]);
			Integer order = Integer.valueOf(tempSplit[2]);
//			if (order ==1){
//				result += score ;
//			}
//			else{
//				result += score / (Math.log(order) / Math.log(2));
//			}
			
			result +=  ( Math.pow(2, score) - 1 ) / ( Math.log(order+1) / Math.log(2) );
			
		}
		return result;
	}

	
	
	
	
}
