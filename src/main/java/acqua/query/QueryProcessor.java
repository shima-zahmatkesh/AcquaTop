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
import acqua.query.join.bkg1.FilteringJoinOperator;
import acqua.query.join.bkg1.GNRUpperBound;
import acqua.query.join.bkg1.LRUWithOutWindowsLocality;
import acqua.query.join.bkg1.OracleJoinOperator;
import acqua.query.join.bkg1.OETJoinOperator;
import acqua.query.join.bkg1.LRUJoinOperator;
import acqua.query.join.bkg1.PrefectSlidingOET;
import acqua.query.join.bkg1.RandomCacheUpdateJoin;
import acqua.query.join.bkg1.RandomWithOutWindowsLocality;
import acqua.query.join.bkg1.ScoringJoinOperator;
import acqua.query.join.bkg1.SlidingOETJoinOperator;
import acqua.query.join.bkg1.WSJUpperBound;
import acqua.query.join.bkg2.OracleDoubleJoinOperator;
import acqua.query.join.bkg2.DoubleBkgJoinOperator;
import acqua.query.result.ResultAnalyser;

public class QueryProcessor {
	JoinOperator join;
	TwitterStreamCollector tsc;	
	ArrayList<HashMap<Long,Integer>> slidedwindows;
	ArrayList<HashMap<Long,Long>> slidedwindowsTime;
//	HashMap<Long, Integer> initialCache;
//	public static long start=1416244306470L;//select min(TIMESTAMP) + 30000 from BKG 
//	public static int windowSize=60;
	
 QueryProcessor(){//class JoinOperator){
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
		
//		if(joinType==1)
//			join=new OracleJoinOperator();
//		if(joinType==2)
//			join=new DWJoinOperator();
//		if(joinType==3)
//			join=new LRUJoinOperator(Config.INSTANCE.getUpdateBudget());//update budget of 10
//		if(joinType==4)
//			join=new RandomCacheUpdateJoin(Config.INSTANCE.getUpdateBudget());
//		if(joinType==5)
//			join=new SlidingOETJoinOperator(Config.INSTANCE.getUpdateBudget(), true);
//		if(joinType==6)
//			join=new PrefectSlidingOET(Config.INSTANCE.getUpdateBudget(), true);
//		if(joinType==7)
//			join=new SlidingOETJoinOperator(Config.INSTANCE.getUpdateBudget(), false);
//		if(joinType==8)
//			join=new PrefectSlidingOET(Config.INSTANCE.getUpdateBudget(), false);
//		if(joinType==9)
//			join=new RandomWithOutWindowsLocality(Config.INSTANCE.getUpdateBudget());
//		if(joinType==10)
//			join=new LRUWithOutWindowsLocality(Config.INSTANCE.getUpdateBudget());
//		if(joinType==11)
//			join=new WSJUpperBound(Config.INSTANCE.getUpdateBudget());
//		if(joinType==12)
//			join=new GNRUpperBound(Config.INSTANCE.getUpdateBudget());
//		if(joinType==13)
//			join=new FilteringJoinOperator(Config.INSTANCE.getUpdateBudget());
//		if(joinType==14)
//			join=new ScoringJoinOperator(Config.INSTANCE.getUpdateBudget());
//		
		
		
		if(joinType==1)
			join=new OracleJoinOperator();
		if(joinType==2)
			join=new DWJoinOperator();
		if(joinType==3)
			join=new FilteringJoinOperator(Config.INSTANCE.getUpdateBudget());
		if(joinType==4)
			join=new RandomCacheUpdateJoin(Config.INSTANCE.getUpdateBudget());
		if(joinType==5)
			join=new SlidingOETJoinOperator(Config.INSTANCE.getUpdateBudget(), true);
		if(joinType==6)
			join=new WSJUpperBound(Config.INSTANCE.getUpdateBudget());
		if(joinType==7)
			join=new PrefectSlidingOET(Config.INSTANCE.getUpdateBudget(), false);
		
		
		long time=Config.INSTANCE.getQueryStartingTime()+Config.INSTANCE.getQueryWindowWidth()*1000;
		int windowCount=0;
		while(windowCount<150){
//			join.process(time,tsc.windows.get(windowCount),null);//TwitterFollowerCollector.getInitialUserFollowersFromDB());//					
			//System.out.println(">>>>>>>>>>>>>>>"+time);
			HashMap<Long,Long> currentCandidateTimeStamp = slidedwindowsTime.get(windowCount);
			join.process(time,slidedwindows.get(windowCount),currentCandidateTimeStamp);//TwitterFollowerCollector.getInitialUserFollowersFromDB());//					
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
		
		QueryProcessor qp=new QueryProcessor();	
		String srcDB = Config.INSTANCE.getDatasetDb();
		String desDB = srcDB.split("\\.")[0]+"_1.db" ;
		Config.INSTANCE.setDatasetDb(desDB);
		
		for(int i = 1 ; i < 4 ; i++){
			System.out.println("--------------------------Evaluation number = " + i + "----------------------");
			qp.evaluateQuery(i);
		}
		ResultAnalyser.analysisExperiment();

		
		
//		for (int j=1 ; j<= Config.INSTANCE.getExperimentIterationNumber() ; j++){
//			 
//			String desDB = srcDB.split("\\.")[0]+"_"+ j +".db" ;
//			Config.INSTANCE.setDatasetDb(desDB);
//			System.out.println("--------------------------- working with database " + desDB + "------------------------------");
//		
//			for(int i=1 ; i < 8 ; i++){
//				System.out.println("--------------------------Evaluation number = " + i + "----------------------");
//				qp.evaluateQuery(i);
//			}	
//			
//			ResultAnalyser.analysisExperiment();
//			if (!renameFile (Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare.csv" , Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"joinOutput/compare_"+ j +".csv") )
//				break;
//			System.out.println("--------------------------Evaluation done for database number"  + j + "----------------------");
//
//		}
//		
//		
//		System.out.println("--------------------------Multiple Evaluation start----------------------");
//		ResultAnalyser.analysisMultipleExperiments();
//		System.out.println("--------------------------Multiple Evaluation end----------------------");

		
	}
}
