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
import java.util.Iterator;
import java.util.TreeMap;

import acqua.config.Config;

public class TopKResultnalyserWithIRMetrics {

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
			createtable (stmt ,  "OMTKNJ");
			createtable (stmt ,  "MTKNTJ");
			createtable (stmt ,  "MTKNFJ");
			createtable (stmt ,  "MTKNAJ");
			createtable (stmt ,  "WSTJ");
			createtable (stmt ,  "RNDJ");
			createtable (stmt ,  "MTKNLRUJ");
			createtable (stmt ,  "MTKNWBMJ");
			createtable (stmt ,  "MTKNRNDJ");
			
			
			putOutputInDatabase( stmt ,"joinOutput/OracleJoinOperatorOutput.txt" , "OTKJ");
			putOutputInDatabase( stmt ,"joinOutput/MTKNOracleJoinOperatorOutput.txt" , "OMTKNJ");
			putOutputInDatabase( stmt ,"joinOutput/MTKNTJoinOperatorOutput.txt" , "MTKNTJ");
			putOutputInDatabase( stmt ,"joinOutput/MTKNFJoinOperatorOutput.txt" , "MTKNFJ");
			putOutputInDatabase( stmt ,"joinOutput/MTKNAllJoinOperatorOutput.txt" , "MTKNAJ");
			putOutputInDatabase( stmt ,"joinOutput/WSTJoinOperatorOutput.txt" , "WSTJ");
			putOutputInDatabase( stmt ,"joinOutput/RNDJoinOperatorOutput.txt" , "RNDJ");
			putOutputInDatabase( stmt ,"joinOutput/MTKNLRUJoinOperatorOutput.txt" , "MTKNLRUJ");
			putOutputInDatabase( stmt ,"joinOutput/MTKNWBMJoinOperatorOutput.txt" , "MTKNWBMJ");	
			putOutputInDatabase( stmt ,"joinOutput/MTKNRNDJoinOperatorOutput.txt" , "MTKNRNDJ");
			
			
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
			BufferedWriter bw=new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/NDCGcompare.csv")));
			TreeMap<Long,Integer> oracleCount=computeOJoin();

			//use nDCG to compute errors
			HashMap<Long,Double> OMTKNError=computeAccuracy(getResultsOfTimestaps("OTKJ") , getResultsOfTimestaps("OMTKNJ") );
			HashMap<Long,Double> WSTError=computeAccuracy(getResultsOfTimestaps("OTKJ") , getResultsOfTimestaps("WSTJ") );
			HashMap<Long,Double> RNDError=computeAccuracy(getResultsOfTimestaps("OTKJ") , getResultsOfTimestaps("RNDJ") );
			HashMap<Long,Double> MTKNTError=computeAccuracy(getResultsOfTimestaps("OTKJ") , getResultsOfTimestaps("MTKNTJ") );
			HashMap<Long,Double> MTKNFError=computeAccuracy(getResultsOfTimestaps("OTKJ") , getResultsOfTimestaps("MTKNFJ") );
			HashMap<Long,Double> MTKNAError=computeAccuracy(getResultsOfTimestaps("OTKJ") , getResultsOfTimestaps("MTKNAJ") );
			
			HashMap<Long,Double> LRUError=computeAccuracy(getResultsOfTimestaps("OTKJ") , getResultsOfTimestaps("MTKNLRUJ") );			
			HashMap<Long,Double> WBMError=computeAccuracy(getResultsOfTimestaps("OTKJ") , getResultsOfTimestaps("MTKNWBMJ") );
			HashMap<Long,Double> MTKNRNDError=computeAccuracy (getResultsOfTimestaps("OTKJ") , getResultsOfTimestaps("MTKNRNDJ") );

			
			Iterator<Long> itO = oracleCount.keySet().iterator();
			bw.write("timestampe,Oracle,Oracle MTKN,WST,RND,MTKN-T,MTKN-F,MTKN-A,MTKN-WBM,MTKN-LRU,MTKN-RND\n");
			Double cOC=0.0, comtke =0.0, cwste=0.0 ,crnde=0.0,cwbme=0.0, clrue=0.0 ,cmtknte =0.0,cmtknfe =0.0 ,cmtknae =0.0  ,cmtknrnde = 0.0;
			while(itO.hasNext()){
				
				long nextTime = itO.next();
				Integer OC=oracleCount.get(nextTime);
				Double omtkne=OMTKNError.get(nextTime);
				Double wste=WSTError.get(nextTime);
				Double rnde=RNDError.get(nextTime);
				Double mtknte=MTKNTError.get(nextTime);
				Double mtknfe=MTKNFError.get(nextTime);
				Double mtknae=MTKNAError.get(nextTime);
				Double wbme=WBMError.get(nextTime);
				Double lrue=LRUError.get(nextTime);
				Double mtknrnde=MTKNRNDError.get(nextTime);

				//cumulative error
				cOC=cOC + OC ;
				comtke = comtke + (omtkne = omtkne==null?0:omtkne) ;
				cwste= cwste + (wste = wste==null?0:wste) ;
				crnde= crnde + (rnde = rnde==null?0:rnde) ;
				cmtknte = cmtknte + (mtknte = mtknte==null?0:mtknte) ;
				cmtknfe = cmtknfe + (mtknfe = mtknfe==null?0:mtknfe) ;
				cmtknae = cmtknae + (mtknae = mtknae==null?0:mtknae) ;
				
				cwbme= cwbme + (wbme= wbme==null?0:wbme) ;
				clrue= clrue + (lrue= lrue==null?0:lrue) ;
				cmtknrnde= cmtknrnde + (mtknrnde = mtknrnde==null?0:mtknrnde) ;
			
				bw.write(nextTime+","+String.format("%.5f",cOC)+
						","+ String.format("%.5f",(comtke==null?0:comtke))+ 
						","+ String.format("%.5f",(cwste==null?0:cwste))+ 
						","+ String.format("%.5f",(crnde==null?0:crnde))+
						","+ String.format("%.5f",(cmtknte==null?0:cmtknte))+ 
						","+ String.format("%.5f",(cmtknfe==null?0:cmtknfe))+ 
						","+ String.format("%.5f",(cmtknae==null?0:cmtknae))+ 
						","+ String.format("%.5f",(cwbme==null?0:cwbme)) +
						","+ String.format("%.5f",(clrue==null?0:clrue)) +
						","+ String.format("%.5f",(cmtknrnde==null?0:cmtknrnde)) +"\n");
				
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

	private static HashMap<Long, Double> computeAccuracy(
			TreeMap<Long, HashMap<Long, String>> resultsOfTimestaps,
			TreeMap<Long, HashMap<Long, String>> resultsOfTimestaps2) {
		// TODO Auto-generated method stub
		return null;
	}
	
}
