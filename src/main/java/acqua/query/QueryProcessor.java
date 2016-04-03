package acqua.query;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import acqua.config.Config;
import acqua.data.TwitterStreamCollector;
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
import acqua.query.join.simpleCombine.LRUFOperator;
import acqua.query.join.simpleCombine.RNDFOperator;
import acqua.query.join.simpleCombine.WBMFOperator;
import acqua.query.result.ResultAnalyser;

public class QueryProcessor {
	JoinOperator join;
	TwitterStreamCollector tsc;	
	ArrayList<HashMap<Long,Integer>> slidedwindows;
	ArrayList<HashMap<Long,Long>> slidedwindowsTime;

	
	QueryProcessor(){
		tsc= new TwitterStreamCollector();
		tsc.extractSlides(Config.INSTANCE.getQueryWindowWidth(),Config.INSTANCE.getQueryWindowSlide(), Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"twitterStream.txt");
		slidedwindows = tsc.aggregateSildedWindowsUser();
		slidedwindowsTime=tsc.aggregateSildedWindowsUserTime();
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
		//RND.F
		if(joinType==8)
			join=new RNDFOperator(Config.INSTANCE.getUpdateBudget());
		//WBM.F
		if(joinType==9)
			join=new WBMFOperator(Config.INSTANCE.getUpdateBudget(), true);
		//LRU.F.TA
		if(joinType==10)
			join=new LRUFTAOperator(Config.INSTANCE.getUpdateBudget());
		//RND.F.TA
		if(joinType==11)
			join=new RNDFTAOperator(Config.INSTANCE.getUpdateBudget());
		//WBM.F.TA
		if(joinType==12)
			join=new WBMFTAOperator(Config.INSTANCE.getUpdateBudget(), true);
		
		
		long time=Config.INSTANCE.getQueryStartingTime()+Config.INSTANCE.getQueryWindowWidth()*1000;
		int windowCount=0;
		while(windowCount<150){

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
		
		oneExperiment ();
		//multiExperiments();
		
		//percentageMultiExperiment();
		//budgetMultiExperiment();
		
		//percentageExperiment();
		//budgetExperiment();			
		
	}
	
	
	private static void oneExperiment(){
		
		QueryProcessor qp=new QueryProcessor();	
		String srcDB = Config.INSTANCE.getDatasetDb();
		
		String desDB = srcDB.split("\\.")[0]+"_test.db" ;
		Config.INSTANCE.setDatasetDb(desDB);
		
		for(int i = 1 ; i < 13 ; i++){
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
		
			for(int i=1 ; i < 13 ; i++){
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
		
		for (int k=1 ; k<= Config.INSTANCE.getDatabaseNumber() ; k++){
		
			for (int j=0 ; j < percentage.length ; j++){
				 
				String desDB = srcDB.split("\\.")[0]+"_"+ k +"_"+ percentage[j] +".db" ;
				Config.INSTANCE.setDatasetDb(desDB);
				System.out.println("--------------------------- working with database " + desDB + "------------------------------");
			
				for(int i=1 ; i < 8 ; i++){
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
						
				for(int i=8 ; i < 13 ; i++){
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
