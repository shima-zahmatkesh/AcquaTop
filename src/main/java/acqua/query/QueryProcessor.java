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
import acqua.query.join.bkg1.OracleJoinOperator;
import acqua.query.join.bkg1.OETJoinOperator;
import acqua.query.join.bkg1.LRUJoinOperator;
import acqua.query.join.bkg1.RandomCacheUpdateJoin;
import acqua.query.join.bkg1.SlidingOETJoinOperator;
import acqua.query.join.bkg2.OracleDoubleJoinOperator;
import acqua.query.join.bkg2.DoubleBkgJoinOperator;

public class QueryProcessor {
	JoinOperator join;
	TwitterStreamCollector tsc;	
	ArrayList<HashMap<Long,Integer>> slidedwindows;
	ArrayList<HashMap<Long,Long>> slidedwindowsTime;
	//HashMap<Long, Integer> initialCache;
//	public static long start=1416244306470L;//select min(TIMESTAMP) + 30000 from BKG 
//	public static int windowSize=60;
	public QueryProcessor(){//class JoinOperator){
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
		if(joinType==1)
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
			join=new SlidingOETJoinOperator(Config.INSTANCE.getUpdateBudget(), false);
		/*if(joinType==7)
			join=new DoubleBkgJoinOperator(Config.INSTANCE.getUpdateBudget());
		if(joinType==8)
			join=new OracleDoubleJoinOperator();
		*/long time=Config.INSTANCE.getQueryStartingTime()+Config.INSTANCE.getQueryWindowWidth()*1000;
		int windowCount=0;
		while(windowCount<10){
//			join.process(time,tsc.windows.get(windowCount),null);//TwitterFollowerCollector.getInitialUserFollowersFromDB());//					
			HashMap<Long,Long> currentCandidateTimeStamp = slidedwindowsTime.get(windowCount);
			join.process(time,slidedwindows.get(windowCount),currentCandidateTimeStamp);//TwitterFollowerCollector.getInitialUserFollowersFromDB());//					
			windowCount++;
			time = time + Config.INSTANCE.getQueryWindowSlide()*1000;			
		}
		join.close();
		
	}
	
	public static void main(String[] args){
		QueryProcessor qp=new QueryProcessor();	
//		qp.evaluateQuery(5);
		for(int i=1;i<7;i++){
			System.out.println(i);
			qp.evaluateQuery(i);
		}
		
	}
}
