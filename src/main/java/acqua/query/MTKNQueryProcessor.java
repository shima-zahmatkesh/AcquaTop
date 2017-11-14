package acqua.query;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import acqua.config.Config;
import acqua.data.TwitterFollowerCollector;
import acqua.data.TwitterStreamCollector;
import acqua.maintenance.ScoringFunction;
import acqua.query.join.MTKN.ApproximateJoinMTKNOperator;
import acqua.query.join.MTKN.MTKNAllJoinOperator;
import acqua.query.join.MTKN.MTKNFJoinOperator;
import acqua.query.join.MTKN.LRUJoinOperator;
import acqua.query.join.MTKN.MTKNLRUJoinOperator;
import acqua.query.join.MTKN.MTKNOracleJoinOperator;
import acqua.query.join.MTKN.MTKNRNDJoinOperator;
import acqua.query.join.MTKN.MTKNTJoinOperator;
import acqua.query.join.MTKN.MTKNWBMJoinOperator;
import acqua.query.join.MTKN.WBMJoinOperator;
import acqua.query.join.MTKN.OracleJoinOperator;
import acqua.query.join.MTKN.RNDJoinOperator;
import acqua.query.join.MTKN.WSTJoinOperator;
import acqua.query.result.TopKResultAnalyser;
import acqua.query.result.TopKResultAnalyserWithIRMetrics;


public class MTKNQueryProcessor {

	
	ApproximateJoinMTKNOperator join;
	TwitterStreamCollector tsc;	
	ArrayList<HashMap<Long,Integer>> slidedwindows;
	ArrayList<HashMap<Long,Long>> slidedwindowsTime;

	
	
	MTKNQueryProcessor(){
	
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
		
		//WST
		if(joinType==1)
			join=new  WSTJoinOperator();
		if (joinType==2)
			join = new OracleJoinOperator();
		if (joinType==3)
			join = new MTKNOracleJoinOperator();
	
		if (joinType==4)
			join = new MTKNTJoinOperator();
		if (joinType==5)
			join = new MTKNFJoinOperator();
		if (joinType==6)
			join = new MTKNAllJoinOperator();
		
		if (joinType==7)
			join = new RNDJoinOperator();
		if (joinType==8)
			join = new LRUJoinOperator();
		if (joinType==9)
			join = new WBMJoinOperator();
		
		if (joinType==10)
			join = new MTKNRNDJoinOperator();
		if (joinType==11)
			join = new MTKNLRUJoinOperator();
		if (joinType==12)
			join = new MTKNWBMJoinOperator();
		
		join.populateMTKN(slidedwindows,slidedwindowsTime);
		//printSlidedwindows();
		
		long time=Config.INSTANCE.getQueryStartingTime()+Config.INSTANCE.getQueryWindowWidth()*1000;  //evaluationtime = end time of each window
		int windowCount=0;
		while(  windowCount < Config.INSTANCE.getExperimentIterationNumber()){

			
			HashMap<Long,Long> currentCandidateTimeStamp = slidedwindowsTime.get(windowCount);
			join.setCurrentWindow(windowCount);
			
			join.process(time,slidedwindows.get(windowCount),currentCandidateTimeStamp);
			
			//join.outputTopKResult() ;
			join.purgeExpiredWindow(windowCount);
			windowCount++;
			time = time + Config.INSTANCE.getQueryWindowSlide()*1000;			
		}
		join.close();

	}
	
	private void printSlidedwindows() {
		
		for (int i = 1 ; i < slidedwindows.size() ; i++){
			System.out.println("\nwindow     "+ i);
			Iterator <Long> it = slidedwindows.get(i).keySet().iterator();
			while(it.hasNext()){
				Long id = it.next();
				System.out.println(id);
			}
		}
		
	}

	public static void main(String[] args){
		
		runDefaultExperiment();
//		runExperimentForDifferentBudgets();
//		Config.INSTANCE.setUpdateBudget(3);
//		runExperimentForDifferentNumberOfChanges();
//		Config.INSTANCE.setN(40);
//		runExperimentForDifferentK();
//		Config.INSTANCE.setK(5);
//		Config.INSTANCE.setDatasetDb("jdbc:sqlite:synthetictestevening.db");
//		runExperimentForDifferentDatasets();
		
	
	}
	
	
	
	
	
	private static void runDefaultExperiment() {
		
		MTKNQueryProcessor qp = new MTKNQueryProcessor();
		Integer [] index = {1,2,3,4,5,6,7,8,9,10,11,12};
		for (int i=0 ; i < index.length; i++){
			System.out.println("----------------Evaluation number = " + i + "-------------------");
			qp.evaluateQuery(index[i]);
		}
		TopKResultAnalyser.analysisExperimentNDCG();
		TopKResultAnalyser.analysisExperimentACCK();

//		TopKResultAnalyserWithIRMetrics.analysisExperimentIRMetrics("accuracy");
//		TopKResultAnalyserWithIRMetrics.analysisExperimentIRMetrics("recall");
//		TopKResultAnalyserWithIRMetrics.analysisExperimentIRMetrics("precision");
//		TopKResultAnalyserWithIRMetrics.analysisExperimentIRMetrics("f1");
	}

	public static void runExperimentForDifferentBudgets(){
		
		Integer [] budget = {1,2,3,4,5,6,7,8,9,10,15,20,25,30};

		for (int j=0 ; j < budget.length ; j++){ 
				
			Config.INSTANCE.setUpdateBudget( budget[j]);
			MTKNQueryProcessor qp = new MTKNQueryProcessor();
				
			for ( int i = 1 ; i <= 12 ; i++){
				System.out.println("----------------Evaluation number = " + i + "-------------------");
				qp.evaluateQuery(i);
			}
			
			TopKResultAnalyser.analysisExperimentNDCG();
			if (!renameFile (Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/NDCGcompare.csv" , Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/NDCGcompare_budget_"+ budget[j] +".csv") )
				break;
			
			TopKResultAnalyser.analysisExperimentACCK();
			if (!renameFile (Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/ACCKcompare.csv" , Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/ACCKcompare_budget_"+ budget[j] +".csv") )
				break;
			
			TopKResultAnalyserWithIRMetrics.analysisExperimentIRMetrics("accuracy");
			if (!renameFile (Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/accuracyCompare.csv" , Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/accuracyCompare_budget_"+ budget[j] +".csv") )
				break;
			TopKResultAnalyserWithIRMetrics.analysisExperimentIRMetrics("precision");
			if (!renameFile (Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/precisionCompare.csv" , Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/precisionCompare_budget_"+ budget[j] +".csv") )
				break;
		}
		TopKResultAnalyser.mergeCompareFilesForBudget( budget ,"NDCG");
		TopKResultAnalyser.mergeCompareFilesForBudget( budget ,"ACCK");
		TopKResultAnalyserWithIRMetrics.mergeCompareFilesForBudget(budget, "accuracy");
		TopKResultAnalyserWithIRMetrics.mergeCompareFilesForBudget(budget, "precision");

	}
	
	
	public static void runExperimentForDifferentNumberOfChanges(){
		
		Long [] change = {0l , 5l, 10l, 20l, 30l, 40l, 50l, 60l, 70l, 80l};

		for (int j=0 ; j < change.length ; j++){ 
				
			Config.INSTANCE.setN(change[j]);
			MTKNQueryProcessor qp = new MTKNQueryProcessor();
				
			for ( int i = 1 ; i <= 12 ; i++){
				System.out.println("----------------Evaluation number = " + i + "-------------------");
				qp.evaluateQuery(i);
			}
			
			TopKResultAnalyser.analysisExperimentNDCG();
			if (!renameFile (Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/NDCGcompare.csv" , Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/NDCGcompare_N_"+ change[j] +".csv") )
				break;
			
			TopKResultAnalyser.analysisExperimentACCK();
			if (!renameFile (Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/ACCKcompare.csv" , Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/ACCKcompare_N_"+ change[j] +".csv") )
				break;
			
			TopKResultAnalyserWithIRMetrics.analysisExperimentIRMetrics("accuracy");
			if (!renameFile (Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/accuracyCompare.csv" , Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/accuracyCompare_N_"+ change[j] +".csv") )
				break;
			TopKResultAnalyserWithIRMetrics.analysisExperimentIRMetrics("precision");
			if (!renameFile (Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/precisionCompare.csv" , Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/precisionCompare_N_"+ change[j] +".csv") )
				break;
		}
		TopKResultAnalyser.mergeCompareFilesForN(change ,"NDCG");
		TopKResultAnalyser.mergeCompareFilesForN(change ,"ACCK");
		TopKResultAnalyserWithIRMetrics.mergeCompareFilesForN(change, "accuracy");
		TopKResultAnalyserWithIRMetrics.mergeCompareFilesForN(change, "precision");
			
	}
	
	public static void runExperimentForDifferentK(){
		
		Long [] K = {5l,10l,15l,20l,25l,30l,35l,40l,45l,50l};

		for (int j=0 ; j < K.length ; j++){ 
				
			Config.INSTANCE.setK(K[j]);
			MTKNQueryProcessor qp = new MTKNQueryProcessor();
				
			for ( int i = 1 ; i <= 12 ; i++){
				System.out.println("----------------Evaluation number = " + i + "-------------------");
				qp.evaluateQuery(i);
			}
			
			TopKResultAnalyser.analysisExperimentNDCG();
			if (!renameFile (Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/NDCGcompare.csv" , Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/NDCGcompare_K_"+ K[j] +".csv") )
				break;
			
			TopKResultAnalyser.analysisExperimentACCK();
			if (!renameFile (Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/ACCKcompare.csv" , Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/ACCKcompare_K_"+ K[j] +".csv") )
				break;
			
			TopKResultAnalyserWithIRMetrics.analysisExperimentIRMetrics("accuracy");
			if (!renameFile (Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/accuracyCompare.csv" , Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/accuracyCompare_K_"+ K[j] +".csv") )
				break;
			TopKResultAnalyserWithIRMetrics.analysisExperimentIRMetrics("precision");
			if (!renameFile (Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/precisionCompare.csv" , Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/precisionCompare_K_"+ K[j] +".csv") )
				break;
		}
		TopKResultAnalyser.mergeCompareFilesForK(K ,"NDCG");
		TopKResultAnalyser.mergeCompareFilesForK(K ,"ACCK");
		TopKResultAnalyserWithIRMetrics.mergeCompareFilesForK( K, "accuracy");
		TopKResultAnalyserWithIRMetrics.mergeCompareFilesForK( K, "precision");
	}

	public static void runExperimentForDifferentDatasets(){
		
		int [] ch = {10,20,40,60,80,100};
		String srcDB = Config.INSTANCE.getDatasetDb();			
		
		for (int j=0 ; j < ch.length ; j++){ 
				
			String desDB = srcDB.split("\\.")[0]+"-ch"+ ch[j] +".db" ;
			Config.INSTANCE.setDatasetDb(desDB);
			
			MTKNQueryProcessor qp = new MTKNQueryProcessor();
				
			for ( int i = 1 ; i <= 12 ; i++){
				System.out.println("----------------Evaluation number = " + i + "-------------------");
				qp.evaluateQuery(i);
			}
			
			TopKResultAnalyser.analysisExperimentNDCG();
			if (!renameFile (Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/NDCGcompare.csv" , Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/NDCGcompare_CH_"+ ch[j] +".csv") )
				break;
			
			TopKResultAnalyser.analysisExperimentACCK();
			if (!renameFile (Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/ACCKcompare.csv" , Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/ACCKcompare_CH_"+ ch[j] +".csv") )
				break;
			
			TopKResultAnalyserWithIRMetrics.analysisExperimentIRMetrics("accuracy");
			if (!renameFile (Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/accuracyCompare.csv" , Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/accuracyCompare_CH_"+ ch[j] +".csv") )
				break;
			TopKResultAnalyserWithIRMetrics.analysisExperimentIRMetrics("precision");
			if (!renameFile (Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/precisionCompare.csv" , Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/precisionCompare_CH_"+ ch[j] +".csv") )
				break;
		}
		TopKResultAnalyser.mergeCompareFilesForDB(ch ,"NDCG");
		TopKResultAnalyser.mergeCompareFilesForDB(ch ,"ACCK");
		TopKResultAnalyserWithIRMetrics.mergeCompareFilesForDB( ch, "accuracy");
		TopKResultAnalyserWithIRMetrics.mergeCompareFilesForDB( ch, "precision");
	}

	public static boolean renameFile(String oldFile , String newFile){
		
		File oldfile =new File(oldFile);
		File newfile =new File(newFile);
		return oldfile.renameTo(newfile);
	}
	
}
