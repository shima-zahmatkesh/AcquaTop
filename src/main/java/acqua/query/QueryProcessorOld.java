package acqua.query;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;

import acqua.config.Config;
import acqua.data.StreamCollector;
import acqua.data.TwitterFollowerCollector;
import acqua.data.TwitterStreamCollector;
import acqua.maintenance.MinTopK;
import acqua.query.join.*;
import acqua.query.join.TACombine.LRUFTAOperator;
import acqua.query.join.TACombine.RNDFTAOperator;
import acqua.query.join.TACombine.WBMFTAOperator;
import acqua.query.join.acqua.WSTJoinOperator;
import acqua.query.join.acqua.FilterJoinOperator;
import acqua.query.join.acqua.LRUJoinOperator;
import acqua.query.join.acqua.OracleJoinOperator;
import acqua.query.join.acqua.RNDJoinOperator;
import acqua.query.join.acqua.WBMJoinOperator;
import acqua.query.join.cache.FilterCOperator;
import acqua.query.join.cache.LRUCOperator;
import acqua.query.join.cache.WBMCOperator;
import acqua.query.join.partialView.FilterPVOperator;
import acqua.query.join.scoringCombine.LRUFSAOperator;
import acqua.query.join.scoringCombine.RNDFSAOperator;
import acqua.query.join.scoringCombine.WBMFSAOneListOperator;
import acqua.query.join.scoringCombine.WBMFSAOperator;
import acqua.query.join.simpleCombine.LRUFOperator;
import acqua.query.join.simpleCombine.RNDFOperator;
import acqua.query.join.simpleCombine.WBMFOperator;
import acqua.query.result.DataPloting;
import acqua.query.result.ResultAnalyser;
import acqua.query.result.TopKResultAnalyser;
import acqua.query.join.topk.*;

public class QueryProcessorOld {
	JoinOperator join;
	TwitterStreamCollector tsc;	
	//StreamCollector tsc;	
	ArrayList<HashMap<Long,Integer>> slidedwindows;
	ArrayList<HashMap<Long,Long>> slidedwindowsTime;

	protected MinTopK minTopK = new MinTopK();
	
	QueryProcessorOld(){
		//tsc= new StreamCollector();
		//tsc.extractSlides(Config.INSTANCE.getQueryWindowWidth(),Config.INSTANCE.getQueryWindowSlide(), Config.INSTANCE.getQueryStartingTime() , Config.INSTANCE.getQueryStartingTime()*1000*Config.INSTANCE.getExperimentIterationNumber() );

		tsc= new TwitterStreamCollector();
		tsc.extractSlides(Config.INSTANCE.getQueryWindowWidth(),Config.INSTANCE.getQueryWindowSlide(), Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"twitterStream.txt");

		slidedwindows = tsc.aggregateSildedWindowsUser();
		slidedwindowsTime=tsc.aggregateSildedWindowsUserTime();
		
		if (Config.INSTANCE.getTopkQuery()){
			ScoringFunction.setMaxFollowerCount((float)TwitterFollowerCollector.getMaximumFollowerCount());
			ScoringFunction.setMinFollowerCount((float)TwitterFollowerCollector.getMinimumFollowerCount());
			ScoringFunction.setMaxMentions((float) tsc.getMaximumUserMentions(slidedwindows));
			ScoringFunction.setMinMentions((float) tsc.getMinimumUserMentions(slidedwindows));

		}
		
		
	}

	public void evaluateQuery(int joinType){
		
		//Oracle
		if(joinType==1)
			join=new OracleJoinOperator();
		//WST
		if(joinType==2)
			join=new WSTJoinOperator();
		//WSJ-RND
		if(joinType==3)
			join=new RNDJoinOperator(Config.INSTANCE.getUpdateBudget());
		//WSJ-WBM
		if(joinType==4)
			join=new WBMJoinOperator(Config.INSTANCE.getUpdateBudget(), true);
		//WSJ-LRU
		if(joinType==5)
			join=new LRUJoinOperator(Config.INSTANCE.getUpdateBudget());
		//Filter
		if(joinType==6)
			join=new FilterJoinOperator(Config.INSTANCE.getUpdateBudget());
		//LRU.F
		if(joinType==7)
			join=new LRUFOperator(Config.INSTANCE.getUpdateBudget());
		//WBM.F
		if(joinType==8)
			join=new WBMFOperator(Config.INSTANCE.getUpdateBudget(), true);
		//RND.F
		if(joinType==9)
			join=new RNDFOperator(Config.INSTANCE.getUpdateBudget());
		
		//LRU.F+
		if(joinType==10)
			join=new LRUFSAOperator(Config.INSTANCE.getUpdateBudget());
		//RND.F+
		if(joinType==12)
			join=new RNDFSAOperator(Config.INSTANCE.getUpdateBudget());
		//WBM.F*
		if(joinType==11)
			join=new WBMFSAOperator(Config.INSTANCE.getUpdateBudget(), true);		
		//WBM.F+
		if(joinType==13)
			join=new WBMFSAOneListOperator(Config.INSTANCE.getUpdateBudget(), true);	
		
		
		//LRU.F.TA
		if(joinType==14)
			join=new LRUFTAOperator(Config.INSTANCE.getUpdateBudget());
		//RND.F.TA
		if(joinType==15)
			join=new RNDFTAOperator(Config.INSTANCE.getUpdateBudget());
		//WBM.F.TA
		if(joinType==16)
			join=new WBMFTAOperator(Config.INSTANCE.getUpdateBudget(), true);
		
		//FILTER.C
		if(joinType==17)
			join=new FilterCOperator(Config.INSTANCE.getUpdateBudget());
		//LRU.C
		if(joinType==18)
			join=new LRUCOperator(Config.INSTANCE.getUpdateBudget());
		//WBM.C
		if(joinType==19)
			join=new WBMCOperator(Config.INSTANCE.getUpdateBudget(), true);
		
		//FILTER.PV
		if(joinType==20)
			join=new FilterPVOperator(Config.INSTANCE.getUpdateBudget());
		
		if (joinType == 21)
			join = new TopKOracleJoinOperator();
		
		
		
		long time=Config.INSTANCE.getQueryStartingTime()+Config.INSTANCE.getQueryWindowWidth()*1000;
		int windowCount=0;
		while(  windowCount < Config.INSTANCE.getExperimentIterationNumber()){

			HashMap<Long,Long> currentCandidateTimeStamp = slidedwindowsTime.get(windowCount);
			join.process(time,slidedwindows.get(windowCount),currentCandidateTimeStamp);				
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
		multiExperiments();   //also for partial view-k
		
		
		//percentageExperiment();
		//budgetExperiment();	
		
		
		//percentageMultiExperiment();
		//budgetMultiExperiment();
		
		//experiments for .F, .F+, and .F* algorithms
		//scoringExperiment();
		//scoringMultiExperiment();
		//scoringMultiExperiment_Selectivity();
		//scoringMultiExperiment_Budget();
		
		//combineExperiments();
		
		//partialViewMultiExperiment_Selectivity();
		
		//correctChangeRate();
		//updateChangeRate();
		
		
		//QueryProcessor qp=new QueryProcessor();	
		//qp.evaluateQuery(21);
		
	}
	
	

	private static void oneExperiment(){

		QueryProcessor qp=new QueryProcessor();
		String srcDB = Config.INSTANCE.getDatasetDb();
		
		String desDB = srcDB.split("\\.")[0]+"_1_60.db" ;
		Config.INSTANCE.setDatasetDb(desDB);
		
		for(int i = 1 ; i < 6 ; i++){
			System.out.println("--------------------------Evaluation number = " + i + "----------------------");
			qp.evaluateQuery(i);
		}
		if ( Config.INSTANCE.getTopkQuery()){
			TopKResultAnalyser.analysisExperimentNDCG();
		}
		else{
			ResultAnalyser.analysisExperimentJaccard();
		}
	}

	private static void multiExperiments(){
		
//		
//		QueryProcessor qp=new QueryProcessor();	
//		String srcDB = Config.INSTANCE.getDatasetDb();
//		
//		for (int j=1 ; j<= Config.INSTANCE.getDatabaseNumber() ; j++){
//			 
//			String desDB = srcDB.split("\\.")[0]+"_"+ j +"_25.db" ;
//			Config.INSTANCE.setDatasetDb(desDB);
//			System.out.println("--------------------------- working with database " + desDB + "------------------------------");
//		
//			for(int i=1 ; i < 7 ; i++){
//				System.out.println("--------------------------Evaluation number = " + i + "----------------------");
//				qp.evaluateQuery(i);
//			}
//			
//			
//			ResultAnalyser.analysisExperimentJaccard();
//			if (!renameFile (Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare.csv" , Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare_"+ j +"_25.csv") )
//				break;
//			System.out.println("--------------------------Evaluation done for database number"  + j + "----------------------");
//
//		}
		
		
		System.out.println("--------------------------Multiple Evaluation start----------------------");
		ResultAnalyser.analysisMultipleExperimentsJaccardMedian("25");
		System.out.println("--------------------------Multiple Evaluation end----------------------");

	}

	private static void percentageMultiExperiment(){
		
		
		QueryProcessor qp=new QueryProcessor();	
		String [] percentage ={"10", "20", "25", "30" , "40" , "50","60", "70" , "80" , "90"};    // { "10", "20", "25", "30" , "40" , "50"};
		String srcDB = Config.INSTANCE.getDatasetDb();			
		
		for (int k=1 ; k<= Config.INSTANCE.getDatabaseNumber() ; k++){
		
			for (int j=0 ; j < percentage.length ; j++){
				 
				String desDB = srcDB.split("\\.")[0]+"_"+ k +"_"+ percentage[j] +".db" ;
				Config.INSTANCE.setDatasetDb(desDB);
				System.out.println("--------------------------- working with database " + desDB + "------------------------------");
			
				for(int i=1 ; i < 10 ; i++){
					System.out.println("--------------------------Evaluation number = " + i + "----------------------");
					qp.evaluateQuery(i);
				}	
				
				ResultAnalyser.analysisExperimentJaccard();
				if (!renameFile (Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare.csv" , Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare_"+ k +"_" + percentage[j] +".csv") )
					break;
				System.out.println("--------------------------Evaluation done for database number"  + j + "----------------------");
	
			}
		}
		
		for (int j=0 ; j < percentage.length ; j++){
			System.out.println("--------------------------Multiple Evaluation start----------------------");
			ResultAnalyser.analysisMultipleExperimentsJaccard(percentage[j]);
			if (!renameFile (Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compareMultipleExperiments.csv" , Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compareMultipleExperiments_" + percentage[j] +".csv") )
				break;
			System.out.println("--------------------------Multiple Evaluation end----------------------");
		}
	}

	private static void budgetMultiExperiment(){
		
		
//		QueryProcessor qp=new QueryProcessor();	
	String [] budget = { "1", "2", "3", "4" , "5" , "6" , "7" };
//		String srcDB = Config.INSTANCE.getDatasetDb();
//		
//		for (int k=1 ; k<= Config.INSTANCE.getDatabaseNumber() ; k++){
//		
//			String desDB = srcDB.split("\\.")[0]+"_"+ k +"_25.db" ;
//			Config.INSTANCE.setDatasetDb(desDB);
//			System.out.println("--------------------------- working with database " + desDB + "------------------------------");
//	
//		
//			for (int j=0 ; j < budget.length ; j++){ 
//				
//				Config.INSTANCE.setUpdateBudget( budget[j]);
//						
//				for(int i=1 ; i < 10 ; i++){
//					System.out.println("--------------------------Evaluation number = " + i + "----------------------");
//					qp.evaluateQuery(i);
//				}	
//				
//				ResultAnalyser.analysisExperimentJaccard();
//				if (!renameFile (Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare.csv" , Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare_"+ k +"_" + budget[j] +".csv") )
//					break;
//				System.out.println("--------------------------Evaluation done for budget"  + budget[j] + "----------------------");
//	
//			}
//		}
//		
		for (int j=0 ; j < budget.length ; j++){
			System.out.println("--------------------------Multiple Evaluation start----------------------");
			ResultAnalyser.analysisMultipleExperimentsJaccardMedian(budget[j]);
			System.out.println("--------------------------Multiple Evaluation end----------------------");
		}
	}

	private static void budgetExperiment(){
		
		
		QueryProcessor qp=new QueryProcessor();	
		String [] budget = { "1", "2", "3", "4" , "5" };
		String srcDB = Config.INSTANCE.getDatasetDb();
		String desDB = srcDB.split("\\.")[0]+"_4_25.db" ;
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

	// check different value of alpha
	private static void scoringExperiment(){
		
		
		QueryProcessor qp=new QueryProcessor();	
		String [] alpha = {"0", "0.05", "0.1", "0.15" , "1",  "0.167", "0.333", "0.5", "0.667" , "0.833" };
		String srcDB = Config.INSTANCE.getDatasetDb();
		String desDB = srcDB.split("\\.")[0]+"_4_25.db" ;
		Config.INSTANCE.setDatasetDb(desDB);
		
		for(int i=1 ; i < 2 ; i++){
			System.out.println("--------------------------Evaluation number = " + i + "----------------------");
			qp.evaluateQuery(i);
		}
		
		ResultAnalyser.analysisExperimentJaccard();
		
		for (int j=0 ; j < alpha.length ; j++){ 
			
			Config.INSTANCE.setAlpha( Float.valueOf( alpha[j] ));
					
			for(int i=10 ; i < 11 ; i++){
				System.out.println("--------------------------Evaluation number = " + i + "----------------------");
				qp.evaluateQuery(i);
			}	
			
			ResultAnalyser.analysisExperimentScoringAlgorithm();
			if (!renameFile (Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare_alpha.csv" , Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare_alpha_"+ alpha[j] +".csv") )
				break;
			System.out.println("--------------------------Evaluation done for budget"  + alpha[j] + "----------------------");
		}
		ResultAnalyser.analysisExperimentScoringAlgorithmMerge(alpha , "25" , "4");
	}
	
	private static void scoringMultiExperiment(){
		
		
		QueryProcessor qp=new QueryProcessor();	
		String [] alpha = { "0.167", "0.333", "0.5", "0.667" , "0.833" };
		String srcDB = Config.INSTANCE.getDatasetDb();
		
		for (int db=1 ; db<= Config.INSTANCE.getDatabaseNumber() ; db++){
		
			String desDB = srcDB.split("\\.")[0]+"_"+ db +"_25.db" ;
			Config.INSTANCE.setDatasetDb(desDB);
			System.out.println("--------------------------- working with database " + desDB + "------------------------------");
	
			for(int i=1 ; i < 10 ; i++){
				System.out.println("--------------------------Evaluation number = " + i + "----------------------");
				qp.evaluateQuery(i);
			}
			
			ResultAnalyser.analysisExperimentJaccard();
		
			for (int j = 0 ; j < alpha.length ; j++){
				
				Config.INSTANCE.setAlpha(Float.valueOf( alpha[j] ));
		
				for(int i=10 ; i < 14 ; i++){
					System.out.println("--------------------------Evaluation number = " + i + "----------------------");
					qp.evaluateQuery(i);
				}
				
				ResultAnalyser.analysisExperimentScoringAlgorithm();
				if (!renameFile (Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare_alpha.csv" , Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare_alpha_"+ db + "_25_" + alpha[j] +".csv") )
					break;
				System.out.println("--------------------------Evaluation done for alpha"  + alpha[j] + "----------------------");
			}
			ResultAnalyser.analysisExperimentScoringAlgorithmMerge(alpha , "25" , String.valueOf(db));
			
		}
		System.out.println("--------------------------Multiple Evaluation start----------------------");
		ResultAnalyser.analysisMultipleExperimentsScoringAlgorithm(alpha ,"25");
		System.out.println("--------------------------Multiple Evaluation end----------------------");
		
	}

	private static void scoringMultiExperiment_Selectivity(){
	
	
	QueryProcessor qp=new QueryProcessor();	
	//String [] alpha = {"0.167", "0.333", "0.5", "0.667" , "0.833" };
	//String [] percentage = { "10", "20", "25", "30" , "40" , "50" , "60" ,"70" ,"80" , "90" };
	String [] alpha = {"0.2" , "0.5"  };
	String [] percentage = { "30"  };

//	String srcDB = Config.INSTANCE.getDatasetDb();
//	
//	for (int db=1 ; db<= Config.INSTANCE.getDatabaseNumber() ; db++){
//	
//		for (int p=0 ; p < percentage.length ; p++){
//			 
//			String desDB = srcDB.split("\\.")[0]+"_"+ db +"_"+ percentage[p] +".db" ;
//			Config.INSTANCE.setDatasetDb(desDB);
//			System.out.println("--------------------------- working with database " + desDB + "------------------------------");
//	
//			for (int a=0 ; a < alpha.length ; a++){
//			
//				if ( a == 0){
//					for(int e=1 ; e < 10 ; e++){
//						System.out.println("--------------------------Evaluation number = " + e + "----------------------");
//						qp.evaluateQuery(e);
//					}
//					ResultAnalyser.analysisExperimentJaccard();
//				}
//			
//				Config.INSTANCE.setAlpha(Float.valueOf( alpha[a] ));
//	
//				for(int e=10 ; e < 14 ; e++){
//		 			System.out.println("--------------------------Evaluation number = " + e + "----------------------");
//					qp.evaluateQuery(e);
//				} 
//			
//				ResultAnalyser.analysisExperimentScoringAlgorithm();
//				if (!renameFile (Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare_alpha.csv" , Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare_alpha_"+ db + "_"+ percentage[p] + "_" + alpha[a] +".csv") )
//					break;
//				System.out.println("--------------------------Evaluation done for alpha"  + alpha[a] + "----------------------");
//			}
//			ResultAnalyser.analysisExperimentScoringAlgorithmMerge(alpha , percentage[p] , String.valueOf(db) );
//		}
//	}
	
	for (int p=0 ; p < percentage.length ; p++){
		System.out.println("--------------------------Multiple Evaluation start----------------------");
		ResultAnalyser.analysisMultipleExperimentsScoringAlgorithm(alpha ,percentage[p]);
		System.out.println("--------------------------Multiple Evaluation end----------------------");
	}
	DataPloting.generateDataForPlotingAllItreations2(110);
		

	}

	private static void scoringMultiExperiment_Budget(){
	
	 
	QueryProcessor qp=new QueryProcessor();	
	String [] alpha = {"0.2", "0.5" };
	String [] budget = {"3", "5"  };
	
//	String [] alpha = {"0.167", "0.333", "0.5", "0.667" , "0.833" };
//	String [] budget = { "1", "2",  "3" , "4" , "5" , "6" ,"7" };
	String srcDB = Config.INSTANCE.getDatasetDb();
	
	for (int db=1 ; db<= Config.INSTANCE.getDatabaseNumber() ; db++){
	
		System.out.println("----------------------------------- working with db " + db + "-------------------------------------");

		for (int b=0 ; b < budget.length ; b++){
			
			Config.INSTANCE.setUpdateBudget( budget[b]);
			 
			String desDB = srcDB.split("\\.")[0]+"_"+ db +"_30.db" ;
			Config.INSTANCE.setDatasetDb(desDB);
			System.out.println("-------------- working with budget " + budget[b] + "------------");
	
			for(int e=1 ; e < 10 ; e++){
				System.out.println("-----Evaluation number = " + e + "-----");
				qp.evaluateQuery(e);
			}
			ResultAnalyser.analysisExperimentJaccard();
			
			
			for (int a=0 ; a < alpha.length ; a++){
			
				Config.INSTANCE.setAlpha(Float.valueOf( alpha[a] ));
	
				for(int e=10 ; e < 14 ; e++){
					System.out.println("-----Evaluation number = " + e + "-----");
					qp.evaluateQuery(e);
				}
			
	
				ResultAnalyser.analysisExperimentScoringAlgorithm();
				if (!renameFile (Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare_alpha.csv" , Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare_alpha_"+ db + "_"+ budget[b] + "_" + alpha[a] +".csv") )
					break;
				System.out.println("-------Evaluation done for alpha"  + alpha[a] + "-----");
			}
			
			ResultAnalyser.analysisExperimentScoringAlgorithmMerge(alpha , budget[b] , String.valueOf(db) );
		}
	}
	
	for (int b=0 ; b < budget.length ; b++){
		System.out.println("--------------------------Multiple Evaluation start----------------------");
		ResultAnalyser.analysisMultipleExperimentsScoringAlgorithm(alpha ,budget[b]);
		System.out.println("--------------------------Multiple Evaluation end----------------------");
	}
			

	}

	private static void partialViewMultiExperiment_Selectivity(){
		
		
		QueryProcessor qp=new QueryProcessor();	
		int [] k = {3, 5, 7, 10, 20, 50, 100, 1000, 10000 };
		String [] percentage = { "10", "20", "25", "30" , "40" , "50" , "60" ,"70" };
		String srcDB = Config.INSTANCE.getDatasetDb();
		
		for (int db=1 ; db<= Config.INSTANCE.getDatabaseNumber() ; db++){
		
			for (int p=0 ; p < percentage.length ; p++){
			
				String desDB = srcDB.split("\\.")[0]+"_"+ db +"_"+ percentage[p] +".db" ;
				Config.INSTANCE.setDatasetDb(desDB);
				System.out.println("--------------------------- working with database " + desDB + "------------------------------");
		
				for(int i=0 ; i < k.length ; i++){
					System.out.println("--------------------------Evaluation for k = " + k[i]+ "----------------------");
					Config.INSTANCE.setKThreshold(k[i]);
					qp.evaluateQuery(1);
					qp.evaluateQuery(20);
					ResultAnalyser.analysisPartialViewExperiment();
					if (!renameFile (Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare.csv" , Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare_k_" + db + "_"+ percentage[p] + "_" + k[i] +".csv") )
						break;
				
				}
				ResultAnalyser.analysisPartialViewExperimentMerge(k, percentage[p], db);
			}
		}			
	
	}
	
	private static void combineExperiments(){
		

		QueryProcessor qp=new QueryProcessor();	
		
		
		//String [] fdft = { "500","1000","50000" };    //Filtering Distance From Threshold
		//String [] fdft = { "62","125","250","750","12500","25000" }; 
		//String [] fdft = { "93","187","375","625","875"}; 
		//String [] fdft = { "77","109","156","218"}; 
		//String [] fdft = { "85","101"}; 
		//String [] fdft= { "140","171","202","234","312","437","562","687","812","937"};
		
		String [] fdft = { "500","1000","50000" ,"62","125","250","750","12500","25000","93","187","375","625","875","77","109","156","218","85","101"};    //Filtering Distance From Threshold

		String [] percentage = {"60"};
		//int [] evaluation = {1,4,5,6,7,8};
		int [] evaluation = {1,7,8};
		
		String srcDB = Config.INSTANCE.getDatasetDb();
		
		for (int k = 0 ; k < fdft.length ; k++){
			
			System.out.println("--------------------------- working with distance threshold =  " + fdft[k] + "------------------------------");
			
			Config.INSTANCE.setDistanceFromThreshold( Long.valueOf(fdft[k]) );
		
			for (int db = 1 ; db <= Config.INSTANCE.getDatabaseNumber() ; db++){
			
				for (int p = 0 ; p < percentage.length ; p++){
					 
					String desDB = srcDB.split("\\.")[0]+"_"+ db +"_"+ percentage[p] +".db" ;
					Config.INSTANCE.setDatasetDb(desDB);
					System.out.println("------------- working with database " + desDB + "---------------");
			
					for(int e=0 ; e < evaluation.length ; e++){
						System.out.println("--------Evaluation number = " + evaluation[e] + "-----------");
						qp.evaluateQuery(evaluation[e]);
					}
					ResultAnalyser.analysisExperimentJaccard();	
					
					if (!renameFile (Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare.csv" , Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare_" + db + "_" + percentage[p] + "_"+ fdft[k] +".csv") )
						break;
					
				}
			}
		}
		
		DataPloting.generateDataForPlotingCombineAlgorithm(75);
	}
	
	
	
	
	// adding the change rate for the users whose change rate is equal to zero based on the information in the DB, 
	// but due to the formula that use this change rate, it can't be equal to zero
	private static void correctChangeRate(){
	
	Connection c = null;
	Statement stmt = null;
	try {
		Class.forName("org.sqlite.JDBC");
		String srcDB = Config.INSTANCE.getDatasetDb();
		String [] percentage = { "10", "20", "25", "30" , "40" , "50" , "60" ,"70" , "80" , "90" };
		
		for (int db=1 ; db<= Config.INSTANCE.getDatabaseNumber() ; db++){
		
			for (int p=0 ; p < percentage.length ; p++){
				 
				String desDB = srcDB.split("\\.")[0]+"_"+ db +"_"+ percentage[p] +".db" ;
				Config.INSTANCE.setDatasetDb(desDB);
		
				c = DriverManager.getConnection(desDB);
		//        c = DriverManager.getConnection(srcDB);
				
		        stmt = c.createStatement();
				String sql = "INSERT INTO USER (USERID,  CHANGERATE)  "+
				"VALUES ( 9007322, 0.0001 ) , "+
				" ( 175295203, 0.0001 ), "+
				" ( 44396034, 0.0001 ), "+
				" ( 219452444, 0.0001 ), "+
				" ( 11831752, 0.0001 ), "+
				" ( 16497947, 0.0001 ), "+
				" ( 231549941, 0.0001 ), "+
				" ( 81632884, 0.0001 ), "+
				" ( 18694134, 0.0001 ), "+
				" ( 182031748, 0.0001 ), "+
				" ( 234484328, 0.0001 ), "+
				" ( 18685577, 0.0001 ), "+
				" ( 38695381, 0.0001 ), "+
				" ( 17820399, 0.0001 ) ";
				
				
				stmt.executeUpdate( sql);
				
				System.out.println("query run on " + desDB);
				stmt.close();
				c.close();
			
			}
		}
	} catch ( Exception e ) {
		System.err.println( e.getClass().getName() + ": " + e.getMessage() );
		System.exit(0);
	}
}

	//for updating change rate in new db
	private static void updateChangeRate() {
		
		WBMJoinOperator join=new WBMJoinOperator(Config.INSTANCE.getUpdateBudget(), true);
		join.createUserTableFromBKG(500000);
		
	}

}
