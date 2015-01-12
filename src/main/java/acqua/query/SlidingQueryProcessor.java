package acqua.query;

import java.util.ArrayList;
import java.util.HashMap;

import acqua.config.Config;
import acqua.data.TwitterStreamCollector;
import acqua.query.join.JoinOperator;
import acqua.query.join.bkg1.OETSlidingJoinOperator;

public class SlidingQueryProcessor {
	JoinOperator join;
	TwitterStreamCollector tsc;
	ArrayList<HashMap<Long,Integer>> slidedwindows;
	
	public SlidingQueryProcessor(){
		tsc= new TwitterStreamCollector();
		System.out.println(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"twitterStream.txt");
		tsc.extractSlides(Config.INSTANCE.getQueryWindowWidth(),Config.INSTANCE.getQueryWindowSlide(), Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"twitterStream.txt");
		slidedwindows=tsc.aggregateSildedWindowsUser();
	}
	
	public void evaluateQuery(int joinType){
		if(joinType==1)
			join=new OETSlidingJoinOperator(10);
		long time=Config.INSTANCE.getQueryStartingTime()+Config.INSTANCE.getQueryWindowWidth()*1000;
		int windowCount=0;
		//ArrayList<HashMap<Long, Integer>> slidedWindows = tsc.aggregateSildedWindowsUser();
		while(windowCount<75){
			//System.out.println(tsc.windows.get(windowCount).size());
			HashMap<Long,Long> currentCandidateTimeStamp = tsc.slidedWindowUsersTimeStamp.get(windowCount);
			//currentCandidateTimeStamp.put(-1L, time);
			join.process(time,slidedwindows.get(windowCount),currentCandidateTimeStamp);//TwitterFollowerCollector.getInitialUserFollowersFromDB());//					
			windowCount++;
			time = time + Config.INSTANCE.getQueryWindowSlide()*1000;				
		}
		join.close();
		
	}
	
	public static void main(String[] args){
		SlidingQueryProcessor qp=new SlidingQueryProcessor();	
		qp.evaluateQuery(1);
		
	}
}
