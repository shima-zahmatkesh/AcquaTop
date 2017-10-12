package acqua.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import acqua.config.Config;
import acqua.data.TwitterFollowerCollector;
import acqua.data.TwitterStreamCollector;
import acqua.maintenance.ScoringFunction;
import acqua.query.join.MTKN.ApproximateJoinMTKNOperator;
import acqua.query.join.MTKN.MTKNOracleJoinOperator;
import acqua.query.join.MTKN.OracleJoinOperator;
import acqua.query.join.MTKN.RNDJoinOperator;
import acqua.query.join.MTKN.WSTJoinOperator;
import acqua.query.result.TopKResultAnalyser;


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
			join = new MTKNOracleJoinOperator();
		if (joinType==3)
			join = new OracleJoinOperator();
		if (joinType==4)
			join = new RNDJoinOperator();
		
		join.populateMTKN(slidedwindows,slidedwindowsTime);
		//printSlidedwindows();
		
		long time=Config.INSTANCE.getQueryStartingTime()+Config.INSTANCE.getQueryWindowWidth()*1000;
		int windowCount=0;
		while(  windowCount < Config.INSTANCE.getExperimentIterationNumber()){

			
			HashMap<Long,Long> currentCandidateTimeStamp = slidedwindowsTime.get(windowCount);
			join.setCurrentWindow(windowCount);
			System.out.println("------------------------current window = " + windowCount+ "-------------------------------------------\n\n");
			join.process(time,slidedwindows.get(windowCount),currentCandidateTimeStamp);
			join.outputTopKResult() ;
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
		
		MTKNQueryProcessor qp = new MTKNQueryProcessor();
		for ( int i = 4 ; i <= 4 ; i++){
			System.out.println("--------------------------------------------Evaluation number = " + i + "-------------------------------------------");
			qp.evaluateQuery(i);
		}
		TopKResultAnalyser.analysisExperimentNDCG();
		
		

	}
	


}
