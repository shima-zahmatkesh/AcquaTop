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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;

import acqua.config.Config;

public class TopKResultAnalyserWithIRMetrics {
	
	private static String header =  "timestampe,Oracle,Oracle MTKN,WST,MTKN-T,MTKN-F,MTKN-A,RND,WBM,LRU,MTKN-RND,MTKN-WBM,MTKN-LRU\n";

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
			createtable (stmt ,  "LRUJ");
			createtable (stmt ,  "WBMJ");
			createtable (stmt ,  "MTKNRNDJ");
			createtable (stmt ,  "MTKNLRUJ");
			createtable (stmt ,  "MTKNWBMJ");
			
			
			putOutputInDatabase( stmt ,"joinOutput/OracleJoinOperatorOutput.txt" , "OTKJ");
			putOutputInDatabase( stmt ,"joinOutput/MTKNOracleJoinOperatorOutput.txt" , "OMTKNJ");
			putOutputInDatabase( stmt ,"joinOutput/MTKNTJoinOperatorOutput.txt" , "MTKNTJ");
			putOutputInDatabase( stmt ,"joinOutput/MTKNFJoinOperatorOutput.txt" , "MTKNFJ");
			putOutputInDatabase( stmt ,"joinOutput/MTKNAllJoinOperatorOutput.txt" , "MTKNAJ");
			putOutputInDatabase( stmt ,"joinOutput/WSTJoinOperatorOutput.txt" , "WSTJ");

			putOutputInDatabase( stmt ,"joinOutput/RNDJoinOperatorOutput.txt" , "RNDJ");
			putOutputInDatabase( stmt ,"joinOutput/LRUJoinOperatorOutput.txt" , "LRUJ");
			putOutputInDatabase( stmt ,"joinOutput/WBMJoinOperatorOutput.txt" , "WBMJ");	
			
			putOutputInDatabase( stmt ,"joinOutput/MTKNRNDJoinOperatorOutput.txt" , "MTKNRNDJ");
			putOutputInDatabase( stmt ,"joinOutput/MTKNLRUJoinOperatorOutput.txt" , "MTKNLRUJ");
			putOutputInDatabase( stmt ,"joinOutput/MTKNWBMJoinOperatorOutput.txt" , "MTKNWBMJ");
			
			
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
	
	public static void analysisExperimentIRMetrics( String metric){
		
		try{
			insertResultToDB();
			BufferedWriter bw=new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/"+ metric +"Compare.csv")));
			TreeMap<Long,Integer> oracleCount=computeOJoin();

			//use nDCG to compute errors
			HashMap<Long,HashMap <String, Float>> OMTKNError=computeIRMetrics(getResultsOfTimestaps("OTKJ") , getResultsOfTimestaps("OMTKNJ") );
			HashMap<Long,HashMap <String, Float>> WSTError=computeIRMetrics(getResultsOfTimestaps("OTKJ") , getResultsOfTimestaps("WSTJ") );
			HashMap<Long,HashMap <String, Float>> MTKNTError=computeIRMetrics(getResultsOfTimestaps("OTKJ") , getResultsOfTimestaps("MTKNTJ") );
			HashMap<Long,HashMap <String, Float>> MTKNFError=computeIRMetrics(getResultsOfTimestaps("OTKJ") , getResultsOfTimestaps("MTKNFJ") );
			HashMap<Long,HashMap <String, Float>> MTKNAError=computeIRMetrics(getResultsOfTimestaps("OTKJ") , getResultsOfTimestaps("MTKNAJ") );

			HashMap<Long, HashMap<String, Float>> RNDError=computeIRMetrics (getResultsOfTimestaps("OTKJ") , getResultsOfTimestaps("RNDJ") );
			HashMap<Long,HashMap <String, Float>> LRUError=computeIRMetrics (getResultsOfTimestaps("OTKJ") , getResultsOfTimestaps("LRUJ") );			
			HashMap<Long,HashMap <String, Float>> WBMError=computeIRMetrics(getResultsOfTimestaps("OTKJ") , getResultsOfTimestaps("WBMJ") );
			
			HashMap<Long,HashMap <String, Float>> MTKNRNDError=computeIRMetrics (getResultsOfTimestaps("OTKJ") , getResultsOfTimestaps("MTKNRNDJ") );
			HashMap<Long,HashMap <String, Float>> MTKNLRUError=computeIRMetrics (getResultsOfTimestaps("OTKJ") , getResultsOfTimestaps("MTKNLRUJ") );			
			HashMap<Long,HashMap <String, Float>> MTKNWBMError=computeIRMetrics(getResultsOfTimestaps("OTKJ") , getResultsOfTimestaps("MTKNWBMJ") );

			
			Iterator<Long> itO = oracleCount.keySet().iterator();
			int itrations = oracleCount.size();
			bw.write(header);
			Double cOC=0.0, comtke =0.0, cwste=0.0 ,crnde=0.0,cwbme=0.0, clrue=0.0 ,cmtknte =0.0,cmtknfe =0.0 ,cmtknae =0.0  ,cmtknrnde = 0.0 ,cmtknlrue = 0.0 ,cmtknwbme = 0.0;;
			while(itO.hasNext()){
				
				long nextTime = itO.next();
				Integer OC=oracleCount.get(nextTime);
				System.out.print(nextTime);
				HashMap <String, Float> omtkne=OMTKNError.get(nextTime);
				HashMap <String, Float> wste=WSTError.get(nextTime);

				HashMap <String, Float> mtknte=MTKNTError.get(nextTime);
				HashMap <String, Float> mtknfe=MTKNFError.get(nextTime);
				HashMap <String, Float> mtknae=MTKNAError.get(nextTime);
				
				HashMap <String, Float> rnde=RNDError.get(nextTime);
				HashMap <String, Float> wbme=WBMError.get(nextTime);
				HashMap <String, Float> lrue=LRUError.get(nextTime);
				
				HashMap <String, Float> mtknrnde=MTKNRNDError.get(nextTime);
				HashMap <String, Float> mtknwbme=MTKNWBMError.get(nextTime);
				HashMap <String, Float> mtknlrue=MTKNLRUError.get(nextTime);

				//cumulative error
				cOC=cOC + OC ;
				comtke = comtke + omtkne.get(metric)  ;
				cwste= cwste + wste.get(metric) ;

				cmtknte = cmtknte + mtknte.get(metric)  ;
				cmtknfe = cmtknfe + mtknfe.get(metric) ;
				cmtknae = cmtknae + mtknae.get(metric) ;
				
				crnde= crnde + rnde.get(metric) ;
				cwbme= cwbme + wbme.get(metric) ;
				clrue= clrue + lrue.get(metric)  ;
				
				cmtknrnde= cmtknrnde + mtknrnde.get(metric)  ;
				cmtknwbme= cmtknwbme + mtknwbme.get(metric) ;
				cmtknlrue= cmtknlrue + mtknlrue.get(metric)  ;
			
				bw.write(nextTime+","+String.format("%.5f",cOC)+
						","+ String.format("%.5f",(comtke==null?0:comtke))+ 
						","+ String.format("%.5f",(cwste==null?0:cwste))+ 
						
						","+ String.format("%.5f",(cmtknte==null?0:cmtknte))+ 
						","+ String.format("%.5f",(cmtknfe==null?0:cmtknfe))+ 
						","+ String.format("%.5f",(cmtknae==null?0:cmtknae))+ 
						","+ String.format("%.5f",(crnde==null?0:crnde))+
						","+ String.format("%.5f",(cwbme==null?0:cwbme)) +
						","+ String.format("%.5f",(clrue==null?0:clrue)) +
						
						","+ String.format("%.5f",(cmtknrnde==null?0:cmtknrnde)) +
						","+ String.format("%.5f",(cmtknwbme==null?0:cmtknwbme)) +
						","+ String.format("%.5f",(cmtknlrue==null?0:cmtknlrue)) +"\n");
				
			}
//			bw.write("avg,"+String.format("%.5f",cOC)+
//					","+ String.format("%.5f",(comtke / itrations))+ 
//					","+ String.format("%.5f",(cwste / itrations))+ 
//
//					","+ String.format("%.5f",(cmtknte / itrations))+ 
//					","+ String.format("%.5f",(cmtknfe / itrations))+ 
//					","+ String.format("%.5f",(cmtknae / itrations))+
//										
//					","+ String.format("%.5f",(crnde / itrations))+
//					","+ String.format("%.5f",(cwbme / itrations)) +
//					","+ String.format("%.5f",(clrue / itrations)) +
//					","+ String.format("%.5f",(cmtknrnde / itrations)) +
//					","+ String.format("%.5f",(cmtknwbme / itrations)) +
//					","+ String.format("%.5f",(cmtknlrue / itrations)) +"\n");
			
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

	private static TreeMap< Long, Long> getTotalResultNumberOfTimestaps(){
		
		TreeMap< Long, Long> result = new TreeMap<Long, Long>();
		InputStream    fis;
		BufferedReader br;
		String line = null;
		try {
			fis = new FileInputStream(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/TotalResultNumber.txt");
			br = new BufferedReader(new InputStreamReader(fis));
			line = null;
			while((line=br.readLine())!=null){
				
				String[] resultNum = line.split(",");
				result.put(Long.parseLong(resultNum[0]) , Long.parseLong(resultNum[1]));
			}
			br.close();
		} catch (Exception e) {e.printStackTrace(); }
		
		return result;
	}
	
	private static HashMap<Long, HashMap <String, Float>> computeIRMetrics( TreeMap<Long, HashMap<Long, String>> original, TreeMap<Long, HashMap<Long, String>> replica) {
		
		
		HashMap<Long, HashMap <String, Float>> result = new HashMap<Long, HashMap <String, Float>>();
		
		TreeMap< Long, Long> totalResult = getTotalResultNumberOfTimestaps();
		
		Iterator <Long> it  = totalResult.keySet().iterator();
		
		while(it.hasNext()){
			int fp=0,fn=0,tp=0,tn=0;
			HashMap <String, Float> tempResult = new HashMap <String, Float>();
			long timestamp = it.next();
			long totalNumber = totalResult.get(timestamp);
			HashMap<Long, String> originalTopKResult = original.get(timestamp);
			HashMap<Long, String> replicaTopKResult = replica.get(timestamp);
			
			Iterator<Long> originalIt= originalTopKResult.keySet().iterator();
			while(originalIt.hasNext()){
				Long userId = originalIt.next();
				if( replicaTopKResult.containsKey(userId)){
					tp++;
				}
				else
					fp++;
			}
			fn = Config.INSTANCE.getK() - tp;  // fn = fp;
			tn = (int) (totalNumber - Config.INSTANCE.getK());
						
			float accuracy = (float)(tp + tn) / totalNumber;
			float recall = (float)tp / Config.INSTANCE.getK();
			float precision = (float)tp / Config.INSTANCE.getK();
			float f1 =(float) 2 * recall* precision /(recall + precision) ; 
			if (recall==0 && precision == 0) f1=0;
			
			System.out.println("timestap = " + timestamp + "f1 = " + f1 + "    tp = " + tp + "  tn = "+ tn + "  p+n = " +totalNumber);
			tempResult.put("accuracy", accuracy);
			tempResult.put("recall", recall);
			tempResult.put("precision", precision);
			tempResult.put("f1", f1);
			
			result.put(timestamp, tempResult);
			
		}
		return result;
	}
	

	public static void mergeCompareFilesForBudget(Integer [] budgets , String metric){
		
		try{
			
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/"+ metric +"CompareTotal_budget.csv")));
			
			bw.write( "budget," + header);
		
			
			for ( int i = 0 ; i< budgets.length ; i++){
				
				BufferedReader br=new BufferedReader(new FileReader(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/"+ metric +"Compare_budget_"+ budgets[i] + ".csv")));
				
				String lastLine = null , temp = null;
				while( (temp = br.readLine()) != null){
					lastLine = temp;
				}
				bw.write( budgets[i] +","+ lastLine + "\n");
				br.close();
			}
			bw.close();
			
		}catch(Exception e){e.printStackTrace();}
	}
	
	public static void mergeCompareFilesForK(Long[] K , String metric){
		
		try{
			
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/"+ metric +"CompareTotal_K.csv")));
			
			bw.write( "K," + header);
		
			
			for ( int i = 0 ; i< K.length ; i++){
				
				BufferedReader br=new BufferedReader(new FileReader(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/"+ metric +"Compare_K_"+ K[i] + ".csv")));
				
				String lastLine = null , temp = null;
				while( (temp = br.readLine()) != null){
					lastLine = temp;
				}
				bw.write( K[i] +","+ lastLine + "\n");
				br.close();
			}
			bw.close();
			
		}catch(Exception e){e.printStackTrace();}
	}

	public static void mergeCompareFilesForN(Long[] N , String metric){
		
		try{
			
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/"+ metric +"CompareTotal_N.csv")));
			
			bw.write( "N," + header);
		
			
			for ( int i = 0 ; i< N.length ; i++){
				
				BufferedReader br=new BufferedReader(new FileReader(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/"+ metric +"Compare_N_"+ N[i] + ".csv")));
				
				String lastLine = null , temp = null;
				while( (temp = br.readLine()) != null){
					lastLine = temp;
				}
				bw.write( N[i] +","+ lastLine + "\n");
				br.close();
			}
			bw.close();
			
		}catch(Exception e){e.printStackTrace();}
	}
	
	public static void mergeCompareFilesForDB ( int[] ch, String metric) {
		try{
			
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/"+ metric +"CompareTotal_CH.csv")));
			
			bw.write( "DB," + header);
		
			
			for ( int i = 0 ; i< ch.length ; i++){
				
				BufferedReader br=new BufferedReader(new FileReader(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/"+ metric +"Compare_CH_"+ ch[i] + ".csv")));
				
				String lastLine = null , temp = null;
				while( (temp = br.readLine()) != null){
					lastLine = temp;
				}
				bw.write( ch[i] +","+ lastLine + "\n");
				br.close();
			}
			bw.close();
			
		}catch(Exception e){e.printStackTrace();}
		
	}
	


}
