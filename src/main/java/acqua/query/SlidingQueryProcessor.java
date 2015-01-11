package acqua.query;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import acqua.config.Config;
import acqua.data.TwitterStreamCollector;
import acqua.query.join.JoinOperator;
import acqua.query.join.bkg1.LRUJoinOperator;
import acqua.query.join.bkg1.DWJoinOperator;
import acqua.query.join.bkg1.OracleJoinOperator;
import acqua.query.join.bkg1.RandomCacheUpdateJoin;
import acqua.query.join.bkg1.SlidingApproximateJoinOperator;
import acqua.query.join.bkg1.SmartJoin;
import acqua.query.join.bkg1.SmartSlidingJoin;
import acqua.query.join.bkg2.DoubleBkgJoinOperator;
import acqua.query.join.bkg2.OracleDoubleJoinOperator;

public class SlidingQueryProcessor {
	JoinOperator join;
	TwitterStreamCollector tsc;
	ArrayList<HashMap<Long,Integer>> slidedwindows;
	
	public SlidingQueryProcessor(){
		tsc= new TwitterStreamCollector();
		tsc.extractSlides(Config.INSTANCE.getQueryWindowWidth(),Config.INSTANCE.getQuerySlideWidth(), "D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj-night/twitterStream.txt");
		slidedwindows=tsc.aggregateSildedWindowsUser();
	}
	
	public void evaluateQuery(int joinType){
		if(joinType==1)
			join=new SmartSlidingJoin(10);
		long time=Config.INSTANCE.getQueryStartingTime();
		int windowCount=0;
		//ArrayList<HashMap<Long, Integer>> slidedWindows = tsc.aggregateSildedWindowsUser();
		while(windowCount<75){
			time = time + Config.INSTANCE.getQueryWindowWidth()*1000;	
			//System.out.println(tsc.windows.get(windowCount).size());
			HashMap<Long,Long> currentCandidateTimeStamp = tsc.slidedWindowUsersTimeStamp.get(windowCount);
			currentCandidateTimeStamp.put(-1L, time);
			join.process(time,slidedwindows.get(windowCount),currentCandidateTimeStamp);//TwitterFollowerCollector.getInitialUserFollowersFromDB());//					
			windowCount++;
		}
		join.close();
		
	}
	
	public static void main(String[] args){
		SlidingQueryProcessor qp=new SlidingQueryProcessor();	
		qp.evaluateQuery(1);
		
	}
}
