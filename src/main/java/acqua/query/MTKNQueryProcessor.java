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
import acqua.query.join.MTKN.MTKNF2KJoinOperator;
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
	static int policyNumber = 12 ;
	
	
	MTKNQueryProcessor(){
	
		tsc= new TwitterStreamCollector();
		String address = Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"twitterStream.txt";
		System.out.println(address);
		tsc.extractSlides(Config.INSTANCE.getQueryWindowWidth(),Config.INSTANCE.getQueryWindowSlide(), address);

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
		if(joinType==2)
			join=new  WSTJoinOperator();
		if (joinType==1)
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

			//System.out.println("--------------window  =" + windowCount + "  ---------------");
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
	
	private  void printSlidedwindows() {
		
		for (int i = 1 ; i < slidedwindows.size() ; i++){
			
			Iterator <Long> it = slidedwindows.get(i).keySet().iterator();
			while(it.hasNext()){
				Long id = it.next();
				System.out.println(i + ","+ id + "," + slidedwindows.get(i).get(id) );
			}
		}
		
	}
	
	private  void printMentionNumber() {
		
		for (int i = 1 ; i < slidedwindows.size() ; i++){
			int MentionNum = 0;
			Iterator <Long> it = slidedwindows.get(i).keySet().iterator();
			while(it.hasNext()){
				Long id = it.next();
				MentionNum+=slidedwindows.get(i).get(id);
			}
			System.out.println(i + ","+ MentionNum);
		}
		
	}
	
	private  void computeMentionNumberPerUser() {
		
		HashMap<Long,Integer> result = new HashMap<Long , Integer>();
		
		for (int i = 1 ; i < slidedwindows.size() ; i++){
			
			Iterator <Long> it = slidedwindows.get(i).keySet().iterator();
			while(it.hasNext()){
				Long id = it.next();
				
				if (result.containsKey(id))
					result.put(id, result.get(id)+ slidedwindows.get(i).get(id));
				else
					result.put(id, slidedwindows.get(i).get(id));
			}
		}
		
		Iterator <Long> it2 = result.keySet().iterator();
		while(it2.hasNext()){
			Long id = it2.next();
			System.out.println( id + "," + result.get(id) );
		}
		
	}

	public static void main(String[] args){
		
//		MTKNQueryProcessor qp = new MTKNQueryProcessor();
//		qp.printSlidedwindows();
//		qp.printMentionNumber();
//		qp.computeMentionNumberPerUser();
		
//		runDefaultExperiment();
//		runExperimentForDifferentBudgets();
//		Config.INSTANCE.setUpdateBudget(3);
//		runExperimentForDifferentNumberOfChanges();
//		Config.INSTANCE.setN(40);
//		runExperimentForDifferentK();
//		Config.INSTANCE.setK(5);
//		Config.INSTANCE.setDatasetDb("jdbc:sqlite:synthetictestevening.db");
//		runExperimentForDifferentDatasets(1);
	
	
	for ( int i = 2 ; i <=5 ; i++){
		
		System.out.println("change dataset " + i);
		Config.INSTANCE.setDatasetDb("jdbc:sqlite:synthetictest-"+i+".db");
		Config.INSTANCE.setK(5);
		runExperimentForDifferentBudgets(i);
		Config.INSTANCE.setUpdateBudget(15);
		runExperimentForDifferentN(i);
		Config.INSTANCE.setN(10);
		runExperimentForDifferentK(i);
		Config.INSTANCE.setK(5);
		Config.INSTANCE.setDatasetDb("jdbc:sqlite:synthetictestevening.db");
		runExperimentForDifferentDatasets(1);		
	}
		
	
}    
	
	
	
	private static void runDefaultExperiment() {
		
		MTKNQueryProcessor qp = new MTKNQueryProcessor();
		Integer [] index = {1,2,3,4,5,6,7,8,9,10,11,12};
		//Integer [] index = {3,4,5};
		for (int i=0 ; i < index.length; i++){
			System.out.println("----------------Evaluation number = " + index[i] + "-------------------");
			qp.evaluateQuery(index[i]);
		}
		TopKResultAnalyser.analysisExperimentNDCG();
		TopKResultAnalyser.analysisExperimentACCK();
		System.out.println("----------------analysis of Evaluation  is finished-------------------");

//		TopKResultAnalyserWithIRMetrics.analysisExperimentIRMetrics("accuracy");
//		TopKResultAnalyserWithIRMetrics.analysisExperimentIRMetrics("recall");
//		TopKResultAnalyserWithIRMetrics.analysisExperimentIRMetrics("precision");
//		TopKResultAnalyserWithIRMetrics.analysisExperimentIRMetrics("f1");
//		TopKResultAnalyserWithIRMetrics.analysisExperimentIRMetrics("avgPrecisionAtK");
		
	}

	public static void runExperimentForDifferentBudgets(int dataset){
		
		Integer [] budget = {1,3,57,10,15,20,25,30};
		
		//Integer [] budget = {7};
		String path = Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder();

		for (int j=0 ; j < budget.length ; j++){ 
				
			Config.INSTANCE.setUpdateBudget( budget[j]);
			MTKNQueryProcessor qp = new MTKNQueryProcessor();
				
			for ( int i = 1 ; i <= policyNumber ; i++){
				System.out.println("----------------Evaluation number = " + i + "-------------------");
				qp.evaluateQuery(i);
			}
			
			TopKResultAnalyser.analysisExperimentNDCG();
			if (!renameFile (path+"joinOutput/NDCGcompare.csv" , path+"joinOutput/NDCGcompare_budget_"+ budget[j] +".csv") )
				break;
			
			TopKResultAnalyser.analysisExperimentACCK();
			if (!renameFile (path +"joinOutput/ACCKcompare.csv" ,path+"joinOutput/ACCKcompare_budget_"+ budget[j] +".csv") )
				break;
			
			TopKResultAnalyserWithIRMetrics.analysisExperimentIRMetrics("accuracy");
			if (!renameFile (path+"joinOutput/accuracyCompare.csv" , path+"joinOutput/accuracyCompare_budget_"+ budget[j] +".csv") )
				break;
			TopKResultAnalyserWithIRMetrics.analysisExperimentIRMetrics("precision");
			if (!renameFile (path+"joinOutput/precisionCompare.csv" , path+"joinOutput/precisionCompare_budget_"+ budget[j] +".csv") )
				break;
			TopKResultAnalyserWithIRMetrics.analysisExperimentIRMetrics("avgPrecisionAtK");
			if (!renameFile (path+"joinOutput/avgPrecisionAtKCompare.csv" , path+"joinOutput/avgPrecisionAtKCompare_budget_"+ budget[j] +".csv") )
				break;
		}
		TopKResultAnalyser.mergeCompareFilesForBudget( budget ,"NDCG");
		renameFile (path+"joinOutput/NDCGCompareTotal_budget.csv" , path+"joinOutput/NDCGCompareTotal_budget_"+ dataset +".csv") ;
			
		TopKResultAnalyser.mergeCompareFilesForBudget( budget ,"ACCK");
		renameFile (path+"joinOutput/ACCKCompareTotal_budget.csv" , path+"joinOutput/ACCKCompareTotal_budget_"+ dataset +".csv") ;
		
		TopKResultAnalyserWithIRMetrics.mergeCompareFilesForBudget(budget, "accuracy");
		renameFile (path+"joinOutput/accuracyCompareTotal_budget.csv" , path+"joinOutput/accuracyCompareTotal_budget_"+ dataset +".csv") ;

		TopKResultAnalyserWithIRMetrics.mergeCompareFilesForBudget(budget, "precision");
		renameFile (path+"joinOutput/precisionCompareTotal_budget.csv" , path+"joinOutput/precisionCompareTotal_budget_"+ dataset +".csv") ;

		TopKResultAnalyserWithIRMetrics.mergeCompareFilesForBudget(budget, "avgPrecisionAtK");
		renameFile (path+"joinOutput/avgPrecisionAtKCompareTotal_budget.csv" , path+"joinOutput/avgPrecisionAtKCompareTotal_budget_"+ dataset +".csv") ;


	}
	
	public static void runExperimentForDifferentN(int dataset){
		
		Long [] change = {0l , 5l, 10l, 20l, 30l, 40l, 50l};
		String path = Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder();

		for (int j=0 ; j < change.length ; j++){ 
				
			Config.INSTANCE.setN(change[j]);
			MTKNQueryProcessor qp = new MTKNQueryProcessor();
				
			for ( int i = 1 ; i <= policyNumber ; i++){
				System.out.println("----------------Evaluation number = " + i + "-------------------");
				qp.evaluateQuery(i);
			}
			
			TopKResultAnalyser.analysisExperimentNDCG();
			if (!renameFile (path+"joinOutput/NDCGcompare.csv" , path+"joinOutput/NDCGcompare_N_"+ change[j] +".csv") )
				break;
			
			TopKResultAnalyser.analysisExperimentACCK();
			if (!renameFile (path+"joinOutput/ACCKcompare.csv" , path+"joinOutput/ACCKcompare_N_"+ change[j] +".csv") )
				break;
			
			TopKResultAnalyserWithIRMetrics.analysisExperimentIRMetrics("accuracy");
			if (!renameFile (path+"joinOutput/accuracyCompare.csv" , path+"joinOutput/accuracyCompare_N_"+ change[j] +".csv") )
				break;
			TopKResultAnalyserWithIRMetrics.analysisExperimentIRMetrics("precision");
			if (!renameFile (path+"joinOutput/precisionCompare.csv" , path+"joinOutput/precisionCompare_N_"+ change[j] +".csv") )
				break;
			TopKResultAnalyserWithIRMetrics.analysisExperimentIRMetrics("avgPrecisionAtK");
			if (!renameFile (path+"joinOutput/avgPrecisionAtKCompare.csv" , path+"joinOutput/avgPrecisionAtKCompare_N_"+ change[j] +".csv") )
				break;
		}
		TopKResultAnalyser.mergeCompareFilesForN(change ,"NDCG");
		renameFile (path+"joinOutput/NDCGCompareTotal_N.csv" , path+"joinOutput/NDCGCompareTotal_N_"+ dataset +".csv") ;

		TopKResultAnalyser.mergeCompareFilesForN(change ,"ACCK");
		renameFile (path+"joinOutput/ACCKCompareTotal_N.csv" , path+"joinOutput/ACCKCompareTotal_N_"+ dataset +".csv") ;

		TopKResultAnalyserWithIRMetrics.mergeCompareFilesForN(change, "accuracy");
		renameFile (path+"joinOutput/accuracyCompareTotal_N.csv" , path+"joinOutput/accuracyCompareTotal_N_"+ dataset +".csv") ;

		TopKResultAnalyserWithIRMetrics.mergeCompareFilesForN(change, "precision");
		renameFile (path+"joinOutput/precisionCompareTotal_N.csv" , path+"joinOutput/precisionCompareTotal_N_"+ dataset +".csv") ;

		TopKResultAnalyserWithIRMetrics.mergeCompareFilesForN(change, "avgPrecisionAtK");
		renameFile (path+"joinOutput/avgPrecisionAtKCompareTotal_N.csv" , path+"joinOutput/avgPrecisionAtKCompareTotal_N_"+ dataset +".csv") ;

			
	}
	
	public static void runExperimentForDifferentK(int dataset){
		
		Long [] K = {2l,5l,7l,10l,15l,20l ,30l,50l};
		String path = Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder();
		
		for (int j=0 ; j < K.length ; j++){ 
				
			Config.INSTANCE.setK(K[j]);
			MTKNQueryProcessor qp = new MTKNQueryProcessor();
				
			for ( int i = 1 ; i <= policyNumber ; i++){
				System.out.println("----------------Evaluation number = " + i + "-------------------");
				qp.evaluateQuery(i);
			}
			
			TopKResultAnalyser.analysisExperimentNDCG();
			if (!renameFile (path+"joinOutput/NDCGcompare.csv" , path+"joinOutput/NDCGcompare_K_"+ K[j] +".csv") )
				break;
			
			TopKResultAnalyser.analysisExperimentACCK();
			if (!renameFile (path+"joinOutput/ACCKcompare.csv" , path+"joinOutput/ACCKcompare_K_"+ K[j] +".csv") )
				break;
			
			TopKResultAnalyserWithIRMetrics.analysisExperimentIRMetrics("accuracy");
			if (!renameFile (path+"joinOutput/accuracyCompare.csv" , path+"joinOutput/accuracyCompare_K_"+ K[j] +".csv") )
				break;
			TopKResultAnalyserWithIRMetrics.analysisExperimentIRMetrics("precision");
			if (!renameFile (path+"joinOutput/precisionCompare.csv" , path+"joinOutput/precisionCompare_K_"+ K[j] +".csv") )
				break;
			TopKResultAnalyserWithIRMetrics.analysisExperimentIRMetrics("avgPrecisionAtK");
			if (!renameFile (path+"joinOutput/avgPrecisionAtKCompare.csv" , path+"joinOutput/avgPrecisionAtKCompare_K_"+ K[j] +".csv") )
				break;
		}
		TopKResultAnalyser.mergeCompareFilesForK(K ,"NDCG");
		renameFile (path+"joinOutput/NDCGCompareTotal_K.csv" , path+"joinOutput/NDCGCompareTotal_K_"+ dataset +".csv") ;

		TopKResultAnalyser.mergeCompareFilesForK(K ,"ACCK");
		renameFile (path+"joinOutput/ACCKCompareTotal_K.csv" , path+"joinOutput/ACCKCompareTotal_K_"+ dataset +".csv") ;

		TopKResultAnalyserWithIRMetrics.mergeCompareFilesForK( K, "accuracy");
		renameFile (path+"joinOutput/accuracyCompareTotal_K.csv" , path+"joinOutput/accuracyCompareTotal_K_"+ dataset +".csv") ;

		TopKResultAnalyserWithIRMetrics.mergeCompareFilesForK( K, "precision");
		renameFile (path+"joinOutput/precisionCompareTotal_K.csv" , path+"joinOutput/precisionCompareTotal_K_"+ dataset +".csv") ;

		TopKResultAnalyserWithIRMetrics.mergeCompareFilesForK( K, "avgPrecisionAtK");
		renameFile (path+"joinOutput/avgPrecisionAtKCompareTotal_K.csv" , path+"joinOutput/avgPrecisionAtKCompareTotal_K_"+ dataset +".csv") ;

	}
	
	public static void runExperimentForDifferentDatasets(int dataset){
		
		int [] ch = {5,10,20,40,80};
		String srcDB = Config.INSTANCE.getDatasetDb();			
		String path = Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder();
		
		for (int j=0 ; j < ch.length ; j++){ 
				
			//String desDB = srcDB.split("\\.")[0]+"-"+ dataset +"-ch"+ ch[j] +".db" ;
			String desDB = srcDB.split("\\.")[0]+"-ch"+ ch[j] +".db" ;
			Config.INSTANCE.setDatasetDb(desDB);
			
			MTKNQueryProcessor qp = new MTKNQueryProcessor();
				
			for ( int i = 1 ; i <= policyNumber ; i++){
				System.out.println("----------------Evaluation number = " + i + "-------------------");
				qp.evaluateQuery(i);
			}
			
			TopKResultAnalyser.analysisExperimentNDCG();
			if (!renameFile (path+"joinOutput/NDCGcompare.csv" , path+"joinOutput/NDCGcompare_CH_"+ ch[j] +".csv") )
				break;
			
			TopKResultAnalyser.analysisExperimentACCK();
			if (!renameFile (path+"joinOutput/ACCKcompare.csv" , path+"joinOutput/ACCKcompare_CH_"+ ch[j] +".csv") )
				break;
			
			TopKResultAnalyserWithIRMetrics.analysisExperimentIRMetrics("accuracy");
			if (!renameFile (path+"joinOutput/accuracyCompare.csv" , path+"joinOutput/accuracyCompare_CH_"+ ch[j] +".csv") )
				break;
			TopKResultAnalyserWithIRMetrics.analysisExperimentIRMetrics("precision");
			if (!renameFile (path+"joinOutput/precisionCompare.csv" , path+"joinOutput/precisionCompare_CH_"+ ch[j] +".csv") )
				break;
			TopKResultAnalyserWithIRMetrics.analysisExperimentIRMetrics("avgPrecisionAtK");
			if (!renameFile (path+"joinOutput/avgPrecisionAtKCompare.csv" , path+"joinOutput/avgPrecisionAtKCompare_CH_"+ ch[j] +".csv") )
				break;
		}
		TopKResultAnalyser.mergeCompareFilesForDB(ch ,"NDCG");
		renameFile (path+"joinOutput/NDCGCompareTotal_CH.csv" , path+"joinOutput/NDCGCompareTotal_CH_"+ dataset +".csv") ;

		TopKResultAnalyser.mergeCompareFilesForDB(ch ,"ACCK");
		renameFile (path+"joinOutput/ACCKCompareTotal_CH.csv" , path+"joinOutput/ACCKCompareTotal_CH_"+ dataset +".csv") ;

		TopKResultAnalyserWithIRMetrics.mergeCompareFilesForDB( ch, "accuracy");
		renameFile (path+"joinOutput/accuracyCompareTotal_CH.csv" , path+"joinOutput/accuracyCompareTotal_CH_"+ dataset +".csv") ;

		TopKResultAnalyserWithIRMetrics.mergeCompareFilesForDB( ch, "precision");
		renameFile (path+"joinOutput/precisionCompareTotal_CH.csv" , path+"joinOutput/precisionCompareTotal_CH_"+ dataset +".csv") ;

		TopKResultAnalyserWithIRMetrics.mergeCompareFilesForDB( ch, "avgPrecisionAtK");
		renameFile (path+"joinOutput/avgPrecisionAtKCompareTotal_CH.csv" , path+"joinOutput/avgPrecisionAtKCompareTotal_CH_"+ dataset +".csv") ;


	}
	
	public static boolean renameFile(String oldFile , String newFile){
		
		File oldfile =new File(oldFile);
		File newfile =new File(newFile);
		return oldfile.renameTo(newfile);
	}
	
}
