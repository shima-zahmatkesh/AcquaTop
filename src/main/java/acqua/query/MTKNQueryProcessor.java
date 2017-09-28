package acqua.query;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;

import acqua.config.Config;
import acqua.data.TwitterFollowerCollector;
import acqua.data.TwitterStreamCollector;
import acqua.maintenance.MinTopK;
import acqua.query.join.MTKN.ApproximateJoinMTKNOperator;
import acqua.query.join.MTKN.WSTJoinOperator;
import acqua.query.join.topk.ScoringFunction;
import acqua.query.join.topk.TopKOracleJoinOperator;
import acqua.query.result.ResultAnalyser;


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
		
		
		join.populateMTKN(slidedwindows,slidedwindowsTime);
		
		long time=Config.INSTANCE.getQueryStartingTime()+Config.INSTANCE.getQueryWindowWidth()*1000;
		int windowCount=0;
		while(  windowCount < Config.INSTANCE.getExperimentIterationNumber()){

			
			HashMap<Long,Long> currentCandidateTimeStamp = slidedwindowsTime.get(windowCount);
			join.setCurrentWindow(windowCount);
			join.process(time,slidedwindows.get(windowCount),currentCandidateTimeStamp);
			join.outputTopKResult() ;
			join.purgeExpiredWindow(windowCount);
			
			windowCount++;
			time = time + Config.INSTANCE.getQueryWindowSlide()*1000;			
		}
		join.close();

	}
	
	public static void main(String[] args){
		
		MTKNQueryProcessor qp = new MTKNQueryProcessor();
		qp.evaluateQuery(1);

	}
	


}
