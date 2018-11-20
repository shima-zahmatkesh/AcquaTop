package acqua.query;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import acqua.*;
import acqua.config.Config;
import acqua.data.RandomStreamParser;
import acqua.data.RemoteBKGManager;
import acqua.data.SlidingWindowFromStreamGeneratorFile;
import acqua.data.TwitterFollowerCollector;
import acqua.data.TwitterStreamCollector;
import acqua.maintenance.ScoringFunction;
import acqua.query.join.*;
import acqua.query.join.MTKN.ApproximateJoinMTKNOperator;
import acqua.query.join.MTKN.LRUJoinOperator;
import acqua.query.join.MTKN.MTKNAllJoinOperator;
import acqua.query.join.MTKN.MTKNFJoinOperator;
import acqua.query.join.MTKN.MTKNLRUJoinOperator;
import acqua.query.join.MTKN.MTKNOracleJoinOperator;
import acqua.query.join.MTKN.MTKNRNDJoinOperator;
import acqua.query.join.MTKN.MTKNTJoinOperator;
import acqua.query.join.MTKN.MTKNWBMJoinOperator;
import acqua.query.join.MTKN.OracleJoinOperator;
import acqua.query.join.MTKN.RNDJoinOperator;
import acqua.query.join.MTKN.WBMJoinOperator;
import acqua.query.join.MTKN.WSTJoinOperator;
import acqua.query.result.TopKResultAnalyser;
import acqua.query.result.TopKResultAnalyserWithIRMetrics;
import acqua.query.window.Entry;
import acqua.query.window.SlidingWindow;
import acqua.query.window.Window;

public class QueryProcessor {

	RandomStreamParser rsp;
	ApproximateJoinMTKNOperator join;
	//TwitterStreamCollector tsc;	
	ArrayList<HashMap<Long,Integer>> slidedwindows;
	ArrayList<HashMap<Long,Long>> slidedwindowsTime;
	static int policyNumber = 12 ;
	
	public QueryProcessor() {
		
	//	if (Config.INSTANCE.getTopkQuery()){
			ScoringFunction.setMaxFollowerCount( (float) RemoteBKGManager.getMaximumValue());
			ScoringFunction.setMinFollowerCount( (float) RemoteBKGManager.getMinimumValue() );
		//	ScoringFunction.setMaxMentions((float) tsc.getMaximumUserMentions(slidedwindows));
		//	ScoringFunction.setMinMentions((float) tsc.getMinimumUserMentions(slidedwindows));
	//	}
	}

	public void evaluateQuery(int joinType) {
		
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
				

		long time = Config.INSTANCE.getQueryStartingTime() + Config.INSTANCE.getQueryWindowWidth();// *1000;
		int windowCount = 0;

		SlidingWindow sw = new SlidingWindowFromStreamGeneratorFile(Config.INSTANCE.getQueryWindowWidth(), Config.INSTANCE.getQueryWindowSlide(), Config.INSTANCE.getQueryStartingTime(), Config.INSTANCE.getProjectPath() + Config.INSTANCE.getStreamFilePath());
		
		while (sw.hasNext()) {
			
			Window w = sw.next();
			
			join.setCurrentWindow(w.getWindowId());
			
			Map<Long, Long> entries = w.getDistinctEntriesAsMap();
			
			join.populateMTKNCurrentWindow((HashMap<Long, Integer>) w.getFrequencyOfEntitiesAsMap() , (HashMap<Long, Long>) entries );
			join.process(w.getEndingTime(), w.getFrequencyOfEntitiesAsMap(), entries);

			join.outputTopKResult() ;
			join.purgeExpiredWindow(windowCount);
			windowCount++;
		}

		join.close();
		
	}
	
private void printSlidedwindows(ArrayList<HashMap<Long,Integer>> slidedwindows) {
		
		for (int i = 0 ; i < slidedwindows.size() ; i++){
			System.out.println("\nwindow     "+ i);
			Iterator <Long> it = slidedwindows.get(i).keySet().iterator();
			while(it.hasNext()){
				Long id = it.next();
				System.out.println(id);
			}
		}
		
	}
	
	public static void main(String[] args) {

		runDefaultExperiment();
 
	}
	
	private static void runDefaultExperiment() {
		
		QueryProcessor qp = new QueryProcessor();
		Integer [] index =  {1,2,3 ,4,5,6,7,8,9,10,11,12};
		
		for (int i=0 ; i < index.length; i++){
			System.out.println("----------------Evaluation number = " + index[i] + "-------------------");
			qp.evaluateQuery(index[i]);
		}
		TopKResultAnalyser.analysisExperimentNDCG();
		TopKResultAnalyser.analysisExperimentACCK();
		TopKResultAnalyserWithIRMetrics.analysisExperimentIRMetrics("precision");

//		TopKResultAnalyserWithIRMetrics.analysisExperimentIRMetrics("accuracy");
//		TopKResultAnalyserWithIRMetrics.analysisExperimentIRMetrics("recall");
//		TopKResultAnalyserWithIRMetrics.analysisExperimentIRMetrics("precision");
//		TopKResultAnalyserWithIRMetrics.analysisExperimentIRMetrics("f1");
//		TopKResultAnalyserWithIRMetrics.analysisExperimentIRMetrics("avgPrecisionAtK");
		
	}
	
	
	
	
	public static ArrayList<HashMap<Long,Long>> GetAllEntries (SlidingWindow sw){
		
		ArrayList<HashMap<Long,Long>> result = new ArrayList<HashMap<Long,Long>> ()  ;
		
		while (sw.hasNext()) {
			
			Window w = sw.next();
			
			HashMap<Long, Long> entries = (HashMap<Long, Long>) w.getDistinctEntriesAsMap();
			System.out.println("entries size"+entries.size());
			result.add(entries);
		}
		return result;
	}
	
	public static ArrayList<HashMap<Long,Integer>> GetAllFrequencies (SlidingWindow sw){
		
		ArrayList<HashMap<Long,Integer>> result = new ArrayList<HashMap<Long,Integer>> ()  ;
		
		while (sw.hasNext()) {
			
			Window w = sw.next();
			HashMap<Long, Integer> entries = (HashMap<Long, Integer>) w.getFrequencyOfEntitiesAsMap();
			result.add(entries);
		}
		return result;
	}
	

	public Integer getMaximumUserMentions(ArrayList<HashMap<Long,Integer>> slidedWindows){
		
		Integer maxMentions = Integer.MIN_VALUE ;
		
		for ( int i = 0 ; i < slidedWindows.size() ; i++){
			
			HashMap<Long,Integer> userMentions = slidedWindows.get(i);
			Iterator<Long> it= userMentions.keySet().iterator();
			while(it.hasNext()){
				Long id=it.next();
				Integer mentions = userMentions.get(id);
				if (mentions > maxMentions){
					maxMentions = mentions ;
				}
			}
		}
		return maxMentions;
	}
	
	public Integer getMinimumUserMentions(ArrayList<HashMap<Long,Integer>> slidedWindows){
		
		Integer minMentions = Integer.MAX_VALUE ;
		
		for ( int i = 0 ; i < slidedWindows.size() ; i++){
			
			HashMap<Long,Integer> userMentions = slidedWindows.get(i);
			Iterator<Long> it= userMentions.keySet().iterator();
			while(it.hasNext()){
				Long id=it.next();
				Integer mentions = userMentions.get(id);
				if (mentions < minMentions){
					minMentions = mentions ;
				}
			}
		}
		return minMentions;
	}
	
	
	
	
	
	
	

}
