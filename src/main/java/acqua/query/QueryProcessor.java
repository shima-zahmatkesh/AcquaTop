package acqua.query;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;

import acqua.*;
import acqua.config.Config;
import acqua.data.TwitterStreamCollector;
import acqua.query.join.*;
import acqua.query.join.bkg1.DWJoinOperator;
import acqua.query.join.bkg1.FilteringJoinOperator;
import acqua.query.join.bkg1.GNRUpperBound;
import acqua.query.join.bkg1.LRUWithOutWindowsLocality;
import acqua.query.join.bkg1.OracleJoinOperator;
import acqua.query.join.bkg1.OETJoinOperator;
import acqua.query.join.bkg1.LRUJoinOperator;
import acqua.query.join.bkg1.PrefectSlidingOET;
import acqua.query.join.bkg1.RandomCacheUpdateJoin;
import acqua.query.join.bkg1.RandomWithOutWindowsLocality;
import acqua.query.join.bkg1.ScoringJoinOperator;
import acqua.query.join.bkg1.SlidingOETJoinOperator;
import acqua.query.join.bkg1.WSJUpperBound;
import acqua.query.join.bkg2.OracleDoubleJoinOperator;
import acqua.query.join.bkg2.DoubleBkgJoinOperator;
import acqua.query.result.ResultAnalyser;

public class QueryProcessor {
	JoinOperator join;
	TwitterStreamCollector tsc;	
	ArrayList<HashMap<Long,Integer>> slidedwindows;
	ArrayList<HashMap<Long,Long>> slidedwindowsTime;
//	HashMap<Long, Integer> initialCache;
//	public static long start=1416244306470L;//select min(TIMESTAMP) + 30000 from BKG 
//	public static int windowSize=60;
	
 QueryProcessor(){//class JoinOperator){
		//updateBudget and join should be initiated
		//join=new 
		
		tsc= new TwitterStreamCollector();
		//tsc.extractWindow(Config.INSTANCE.getQueryWindowWidth(), Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"twitterStream.txt");		
		tsc.extractSlides(Config.INSTANCE.getQueryWindowWidth(),Config.INSTANCE.getQueryWindowSlide(), Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"twitterStream.txt");
		slidedwindows = tsc.aggregateSildedWindowsUser();
		slidedwindowsTime=tsc.aggregateSildedWindowsUserTime();
		//for (int i=0;i<tsc.windows.size();i++)
		//System.out.println(tsc.windows.get(i).size());
		//initialCache = tfc.getFollowerListFromDB(start); //gets the first window
	}

	public void evaluateQuery(int joinType){
		
/*		if(joinType==1)
			join=new OracleJoinOperator();
		if(joinType==2)
			join=new DWJoinOperator();
		if(joinType==3)
			join=new LRUJoinOperator(Config.INSTANCE.getUpdateBudget());//update budget of 10
		if(joinType==4)
			join=new RandomCacheUpdateJoin(Config.INSTANCE.getUpdateBudget());
		if(joinType==5)
			join=new SlidingOETJoinOperator(Config.INSTANCE.getUpdateBudget(), true);
		if(joinType==6)
			join=new PrefectSlidingOET(Config.INSTANCE.getUpdateBudget(), true);
		if(joinType==7)
			join=new SlidingOETJoinOperator(Config.INSTANCE.getUpdateBudget(), false);
		if(joinType==8)
			join=new PrefectSlidingOET(Config.INSTANCE.getUpdateBudget(), false);
		if(joinType==9)
			join=new RandomWithOutWindowsLocality(Config.INSTANCE.getUpdateBudget());
		if(joinType==10)
			join=new LRUWithOutWindowsLocality(Config.INSTANCE.getUpdateBudget());
		if(joinType==11)
			join=new WSJUpperBound(Config.INSTANCE.getUpdateBudget());
		if(joinType==12)
			join=new GNRUpperBound(Config.INSTANCE.getUpdateBudget());
		if(joinType==13)
			join=new FilteringJoinOperator(Config.INSTANCE.getUpdateBudget());
		if(joinType==14)
			join=new ScoringJoin	Operator(Config.INSTANCE.getUpdateBudget());
*/	
		//Oracle
		if(joinType==1)
			join=new OracleJoinOperator();
		//WST
		if(joinType==2)
			join=new DWJoinOperator();
		//Filter
		if(joinType==3)
			join=new FilteringJoinOperator(Config.INSTANCE.getUpdateBudget());
		//WSJ-RND
		if(joinType==4)
			join=new RandomCacheUpdateJoin(Config.INSTANCE.getUpdateBudget());
		//WSJ-WBM
		if(joinType==5)
			join=new SlidingOETJoinOperator(Config.INSTANCE.getUpdateBudget(), true);
		//WSJ-LRU
		if(joinType==6)
			join=new LRUJoinOperator(Config.INSTANCE.getUpdateBudget());
		//WSJ-WBM*
		if(joinType==7)
			join=new PrefectSlidingOET(Config.INSTANCE.getUpdateBudget(), true);
		
		
		long time=Config.INSTANCE.getQueryStartingTime()+Config.INSTANCE.getQueryWindowWidth()*1000;
		int windowCount=0;
		while(windowCount<150){
//			join.process(time,tsc.windows.get(windowCount),null);//TwitterFollowerCollector.getInitialUserFollowersFromDB());//					
			//System.out.println(">>>>>>>>>>>>>>>"+time);
			HashMap<Long,Long> currentCandidateTimeStamp = slidedwindowsTime.get(windowCount);
			join.process(time,slidedwindows.get(windowCount),currentCandidateTimeStamp);//TwitterFollowerCollector.getInitialUserFollowersFromDB());//					
			windowCount++;
			time = time + Config.INSTANCE.getQueryWindowSlide()*1000;			
		}
		join.close();

		

	}
	
	public static boolean renameFile(String oldFile , String newFile){
	
		File oldfile =new File(oldFile);
		File newfile =new File(newFile);
		return oldfile.renameTo(newfile);
	}
	
	public static void main(String[] args){
		
		//oneExperiment ();
		//multiExperiments();
		
		//percentageMultiExperiment();
		budgetMultiExperiment();
		
		//percentageExperiment();
		//budgetExperiment();
				
		
	}
	
	
	private static void oneExperiment (){
		
		QueryProcessor qp=new QueryProcessor();	
		String srcDB = Config.INSTANCE.getDatasetDb();
		
		String desDB = srcDB.split("\\.")[0]+"_4.db" ;
		Config.INSTANCE.setDatasetDb(desDB);
		
		for(int i = 1 ; i < 8 ; i++){
			System.out.println("--------------------------Evaluation number = " + i + "----------------------");
			qp.evaluateQuery(i);
		}
		ResultAnalyser.analysisExperimentJaccard();
	}

	private static void multiExperiments(){
		
		
		QueryProcessor qp=new QueryProcessor();	
		String srcDB = Config.INSTANCE.getDatasetDb();
		
		for (int j=1 ; j<= Config.INSTANCE.getDatabaseNumber() ; j++){
			 
			String desDB = srcDB.split("\\.")[0]+"_"+ j +"_25.db" ;
			Config.INSTANCE.setDatasetDb(desDB);
			System.out.println("--------------------------- working with database " + desDB + "------------------------------");
		
			for(int i=1 ; i < 8 ; i++){
				System.out.println("--------------------------Evaluation number = " + i + "----------------------");
				qp.evaluateQuery(i);
			}	
			
			ResultAnalyser.analysisExperimentJaccard();
			if (!renameFile (Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare.csv" , Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare_"+ j +"_25.csv") )
				break;
			System.out.println("--------------------------Evaluation done for database number"  + j + "----------------------");

		}
		
		
		System.out.println("--------------------------Multiple Evaluation start----------------------");
		ResultAnalyser.analysisMultipleExperimentsJaccard(25);
		System.out.println("--------------------------Multiple Evaluation end----------------------");

	}

	private static void percentageMultiExperiment(){
		
		
		QueryProcessor qp=new QueryProcessor();	
		String [] percentage = { "10", "20", "25", "30" , "40" , "50"};
		String srcDB = Config.INSTANCE.getDatasetDb();
		
//		for (int k=1 ; k<= Config.INSTANCE.getExperimentIterationNumber() ; k++){
//		
//			for (int j=0 ; j < percentage.length ; j++){
//				 
//				String desDB = srcDB.split("\\.")[0]+"_"+ k +"_"+ percentage[j] +".db" ;
//				Config.INSTANCE.setDatasetDb(desDB);
//				System.out.println("--------------------------- working with database " + desDB + "------------------------------");
//			
//				for(int i=1 ; i < 8 ; i++){
//					System.out.println("--------------------------Evaluation number = " + i + "----------------------");
//					qp.evaluateQuery(i);
//				}	
//				
//				ResultAnalyser.analysisExperimentJaccard();
//				if (!renameFile (Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare.csv" , Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare_"+ k +"_" + percentage[j] +".csv") )
//					break;
//				System.out.println("--------------------------Evaluation done for database number"  + j + "----------------------");
//	
//			}
//		}
		
		for (int j=0 ; j < percentage.length ; j++){
			System.out.println("--------------------------Multiple Evaluation start----------------------");
			ResultAnalyser.analysisMultipleExperimentsJaccard(Integer.parseInt(percentage[j]));
			if (!renameFile (Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compareMultipleExperiments.csv" , Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compareMultipleExperiments_" + percentage[j] +".csv") )
				break;
			System.out.println("--------------------------Multiple Evaluation end----------------------");
		}
	}

	private static void budgetMultiExperiment(){
		
		
		QueryProcessor qp=new QueryProcessor();	
		String [] budget = { "1", "2", "3", "4" , "5" };
		String srcDB = Config.INSTANCE.getDatasetDb();
		
		for (int k=1 ; k<= Config.INSTANCE.getDatabaseNumber() ; k++){
		
			String desDB = srcDB.split("\\.")[0]+"_"+ k +"_25.db" ;
			Config.INSTANCE.setDatasetDb(desDB);
			System.out.println("--------------------------- working with database " + desDB + "------------------------------");
	
		
			for (int j=0 ; j < budget.length ; j++){ 
				
				Config.INSTANCE.setUpdateBudget( budget[j]);
						
				for(int i=1 ; i < 8 ; i++){
					System.out.println("--------------------------Evaluation number = " + i + "----------------------");
					qp.evaluateQuery(i);
				}	
				
				ResultAnalyser.analysisExperimentJaccard();
				if (!renameFile (Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare.csv" , Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare_"+ k +"_" + budget[j] +".csv") )
					break;
				System.out.println("--------------------------Evaluation done for budget"  + budget[j] + "----------------------");
	
			}
		}
		
		for (int j=0 ; j < budget.length ; j++){
			System.out.println("--------------------------Multiple Evaluation start----------------------");
			ResultAnalyser.analysisMultipleExperimentsJaccard(Integer.parseInt(budget[j]));
			if (!renameFile (Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compareMultipleExperiments.csv" , Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compareMultipleExperiments_" + budget[j] +".csv") )
				break;
			System.out.println("--------------------------Multiple Evaluation end----------------------");
		}
	}

	private static void budgetExperiment(){
		
		
		QueryProcessor qp=new QueryProcessor();	
		String [] budget = { "1", "2", "3", "4" , "5" };
		String srcDB = Config.INSTANCE.getDatasetDb();
		String desDB = srcDB.split("\\.")[0]+"_4.db" ;
		Config.INSTANCE.setDatasetDb(desDB);
		
		for (int j=0 ; j < budget.length ; j++){ 
			
			Config.INSTANCE.setUpdateBudget( budget[j]);
					
			for(int i=1 ; i < 8 ; i++){
				System.out.println("--------------------------Evaluation number = " + i + "----------------------");
				qp.evaluateQuery(i);
			}	
			
			ResultAnalyser.analysisExperimentJaccard();
			if (!renameFile (Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare.csv" , Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare_budget_"+ budget[j] +".csv") )
				break;
			System.out.println("--------------------------Evaluation done for budget"  + budget[j] + "----------------------");

		}
	}
	
	private static void percentageExperiment(){
		
		
		QueryProcessor qp=new QueryProcessor();	
		String [] percentage = { "10", "20", "25", "30" , "40" , "50"};
		String srcDB = Config.INSTANCE.getDatasetDb();
		
		for (int j=0 ; j < percentage.length ; j++){
			 
			String desDB = srcDB.split("\\.")[0]+"_4_"+ percentage[j] +".db" ;
			Config.INSTANCE.setDatasetDb(desDB);
			System.out.println("--------------------------- working with database " + desDB + "------------------------------");
		
			for(int i=1 ; i < 8 ; i++){
				System.out.println("--------------------------Evaluation number = " + i + "----------------------");
				qp.evaluateQuery(i);
			}	
			
			ResultAnalyser.analysisExperimentJaccard();
			if (!renameFile (Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare.csv" , Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare_percentage_"+ percentage[j] +".csv") )
				break;
			System.out.println("--------------------------Evaluation done for database number"  + j + "----------------------");

		}
	}

	

}
