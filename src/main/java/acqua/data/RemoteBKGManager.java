package acqua.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import acqua.config.Config;

public class RemoteBKGManager {
	public static final RemoteBKGManager INSTANCE = new RemoteBKGManager();

	private RemoteBKGManager() {
		try {
			Class.forName("org.sqlite.JDBC");
		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(0);
		}
		changeRate = getStockChaneRateFromDB();
		// System.out.println(Config.INSTANCE.getProjectPath() + Config.INSTANCE.getBipartitePath());

		//bipartiteMappingGraph = new biadjacencyMatrix(Config.INSTANCE.getProjectPath() + Config.INSTANCE.getBipartitePath());
	}

	//private biadjacencyMatrix bipartiteMappingGraph;
	private Map<Long, Double> changeRate;

	//public biadjacencyMatrix getBipartiteMappingGraph() {
	//	return bipartiteMappingGraph;
	//}

	public Map<Long, Double> getChangeRate() {
		return changeRate;
	}

	public HashMap<Long, Double> getStockChaneRateFromDB() {
		HashMap<Long, Double> result = new HashMap<Long, Double>();
		Connection c = null;
		Statement stmt = null;
		try {
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());
			c.setAutoCommit(true);
			stmt = c.createStatement();
			String sql = "SELECT stockId,sChangeR  FROM SERVICEDS ";
			System.out.println(sql);
			ResultSet rs = stmt.executeQuery(sql);

			while (rs.next()) {
				long replicaUnit = rs.getInt("stockId");
				double replicaUnitChangeRate = rs.getInt("sChangeR");
				result.put(replicaUnit, replicaUnitChangeRate);
			}

			rs.close();
			stmt.close();
			c.close();
		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(0);
		}
		return result;
	}

//	public void generateLocalView() {
//		Connection c = null;
//		Statement stmt = null;
//		try {
//			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());
//			// c.setAutoCommit(true);
//			stmt = c.createStatement();
//			String sql = "DROP TABLE IF EXISTS `SERVICEDS`;CREATE TABLE  `SERVICEDS` ( " + " `stockId`           INT    NOT NULL, " + " `sChangeR`           INT    NOT NULL, " + " `validFrom`           INT    NOT NULL, " + " `validTo`            INT     NOT NULL, "
//					+ " `validValue`           INT    NOT NULL); ";
//			System.out.println(sql);
//			stmt.executeUpdate(sql);
//
//			Random rand = new Random(System.currentTimeMillis());
//			BufferedReader br = new BufferedReader(new FileReader(new File(Config.INSTANCE.getProjectPath() + "/data/changeRate/rbeta100,50,1")));
//			// assign the change rate to each stock and write down its values based on change rate into database
//			// stock should change in intervals larger than slide size of time unit
//			for (int i = 0; i < Config.INSTANCE.getStockVerticesCount(); i++) {
//				String Line = br.readLine();
//				// String [] crid= Line.split("\t");
//				// generate a random number for change intervals of the current stock and preferably smaller than end of stream
//				// int currentStockChangeIntervalTimeUnit = (int)(Double.parseDouble(Line)*Config.INSTANCE.getStreamEndingTime()/2);
//				int currentStockChangeIntervalTimeUnit = (int) (rand.nextInt(Config.INSTANCE.getStreamEndingTime()) / 10);
//				// int currentStockChangeIntervalTimeUnit = rand.nextInt(198);
//				// currentStockChangeIntervalTimeUnit+=2;
//				// int currentStockChangeIntervalTimeUnit = Config.INSTANCE.getStreamEndingTime()/2-(int)Math.max(1, Math.min(Config.INSTANCE.getStreamEndingTime()/2, (int)
//				// Config.INSTANCE.getStreamEndingTime()*3/8 + rand.nextGaussian() * 30));
//				// rand.nextInt(Config.INSTANCE.getStreamEndingTime());// Config.INSTANCE.getQueryWindowSlide() +
//				// rand.nextInt(Config.INSTANCE.getStreamEndingTime() -
//				// Config.INSTANCE.getQueryWindowSlide());
//				int sValue = rand.nextInt(50);
//				for (int t = 0; t * currentStockChangeIntervalTimeUnit < Config.INSTANCE.getStreamEndingTime() + Config.INSTANCE.getQueryWindowWidth(); t++) {
//					sValue++;
//					int validfrom = t * currentStockChangeIntervalTimeUnit;
//					sql = "INSERT INTO SERVICEDS (stockId,sChangeR,validFrom,validTo,validValue) " + "VALUES (" + i + "," + currentStockChangeIntervalTimeUnit + "," + validfrom + "," + (t + 1) * currentStockChangeIntervalTimeUnit + "," + sValue + ")";
//					// sql = "INSERT INTO SERVICEDS (stockId,sChangeR,validFrom,validTo,validValue) " +
//					// "VALUES (?,?,?,?,?)";
//					// " + i + "," + currentStockChangeIntervalTimeUnit + "," + t * currentStockChangeIntervalTimeUnit + "," + (t + 1) * currentStockChangeIntervalTimeUnit + "," + sValue + "
//					try {
//						stmt.executeUpdate(sql);
//					} catch (Exception ee) {
//						System.out.println(sql);
//						ee.printStackTrace();
//					}
//				}
//
//				// String commitSQL = "commit;";
//				// System.out.println(commitSQL);
//				// stmt.executeUpdate(commitSQL);
//				//
//			}
//			stmt.close();
//			c.close();
//		} catch (Exception e) {
//			System.err.println(e.getClass().getName() + ": " + e.getMessage());
//			System.exit(0);
//		}
//	}

	public HashMap<Long, String> getInitialBkgInfoFromDB() {
		HashMap<Long, String> result = new HashMap<Long, String>();
		Connection c = null;
		Statement stmt = null;
		try {
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());
			c.setAutoCommit(true);
			stmt = c.createStatement();
			String sql = "SELECT stockId, validValue, validFrom FROM SERVICEDS where validFrom=0";
			System.out.println(sql);
			ResultSet rs = stmt.executeQuery(sql);

			while (rs.next()) {
				long replicaUnit = rs.getInt("stockId");
				int replicaUnitValue = rs.getInt("validValue");
				int replicaValidTo = rs.getInt("validFrom");
				result.put(replicaUnit, replicaUnitValue + "," + replicaValidTo);
			}
			rs.close();
			stmt.close();
			c.close();
		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(0);
		}
		return result;
	}

	public HashMap<Long, Integer> getAllCurrentStockRevenue(long evaluationTime) {
		HashMap<Long, Integer> result = new HashMap<Long, Integer>();
		;
		Connection c = null;
		Statement stmt = null;
		try {
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());
			c.setAutoCommit(true);
			stmt = c.createStatement();
			String sql = "SELECT stockId, validValue FROM SERVICEDS where SERVICEDS.validFrom <= " + evaluationTime + " and SERVICEDS.validTo > " + evaluationTime;
			// System.out.println(sql);
			ResultSet rs = stmt.executeQuery(sql);

			while (rs.next()) {
				long replicaUnit = rs.getLong("stockId");
				int replicaUnitValue = rs.getInt("validValue");
				result.put(replicaUnit, replicaUnitValue);
			}
			rs.close();
			stmt.close();
			c.close();
		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(0);
		}
		return result;
	}

	public int getCurrentStockRevenueFromDB(long evaluationTime, long id) {
		int result = 0;
		Connection c = null;
		Statement stmt = null;
		try {
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());
			c.setAutoCommit(true);
			stmt = c.createStatement();
			String sql = "SELECT stockId, validValue FROM SERVICEDS where SERVICEDS.validFrom <= " + evaluationTime + " and SERVICEDS.validTo > " + evaluationTime + " and stockID = " + id;
			// System.out.println(sql);
			ResultSet rs = stmt.executeQuery(sql);

			while (rs.next()) {
				long replicaUnit = rs.getLong("stockId");
				int replicaUnitValue = rs.getInt("validValue");
				result = replicaUnitValue;
			}
			rs.close();
			stmt.close();
			c.close();
		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(0);
		}
		return result;
	}
	
	public static Long getMinimumValue(){
		Connection c = null;
		Statement stmt = null;
		long followerCount =0;
		try {
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());
			c.setAutoCommit(false);
			stmt = c.createStatement();
			String sql="SELECT  MIN(validValue) AS MINFC  FROM SERVICEDS ";
			ResultSet rs = stmt.executeQuery( sql);
			followerCount  = rs.getLong("MINFC");
			rs.close();
			stmt.close();
			c.close();
		} catch ( Exception e ) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			System.exit(0);
		}
		return followerCount;
	}
	
	public static Long getMaximumValue(){
		Connection c = null;
		Statement stmt = null;
		long followerCount =0;
		try {
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());
			c.setAutoCommit(false);
			stmt = c.createStatement();
			String sql="SELECT  MAX(validValue) AS MAXFC  FROM SERVICEDS ";
			ResultSet rs = stmt.executeQuery( sql);
			followerCount  = rs.getLong("MAXFC");
			rs.close();
			stmt.close();
			c.close();
		} catch ( Exception e ) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			System.exit(0);
		}
		return followerCount;
	}

	public static void main(String args[]) {
	//	RemoteBKGManager.INSTANCE.generateLocalView();
		// System.out.println(RemoteBKGManager.INSTANCE.getCurrentStockRevenueFromDB(150, 0));
	}
}
