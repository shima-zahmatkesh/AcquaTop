package acqua.query;

import java.util.ArrayList;
import java.util.HashMap;

import acqua.config.Config;
import acqua.data.TwitterStreamCollector;
import acqua.query.join.JoinOperator;
import acqua.query.join.bkg1.BaselineJoinOperator;
import acqua.query.join.bkg1.DWJoinOperator;
import acqua.query.join.bkg1.OracleJoinOperator;
import acqua.query.join.bkg1.RandomCacheUpdateJoin;
import acqua.query.join.bkg1.SmartJoin;
import acqua.query.join.bkg2.DoubleBkgJoinOperator;
import acqua.query.join.bkg2.OracleDoubleJoinOperator;

public class SlidingQueryProcessor {
	JoinOperator join;
	TwitterStreamCollector tsc;
	
	public SlidingQueryProcessor(){
		tsc= new TwitterStreamCollector();
		tsc.extractSlides(Config.INSTANCE.getQueryWindowWidth(),Config.INSTANCE.getQuerySlideWidth(), "D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/twitterStream.txt");		
	}
	
	public void evaluateQuery(int joinType){
		if(joinType==1)
			join=new OracleJoinOperator();
		if(joinType==2)
			join=new DWJoinOperator();
		if(joinType==3)
			join=new BaselineJoinOperator(3);//update budget of 10
		if(joinType==4)
			join=new RandomCacheUpdateJoin(3);
		if(joinType==5)
			join=new SmartJoin(3);
		if(joinType==6)
			join=new DoubleBkgJoinOperator(3);
		if(joinType==7)
			join=new OracleDoubleJoinOperator();
		long time=Config.INSTANCE.getQueryStartingTime();
		int windowCount=0;
		ArrayList<HashMap<Long, Integer>> slidedWindows = tsc.aggregateSildedWindowsUser();
		while(windowCount<100){
			time = time + Config.INSTANCE.getQueryWindowWidth()*1000;	
			//System.out.println(tsc.windows.get(windowCount).size());
			join.process(time,slidedWindows.get(windowCount));//TwitterFollowerCollector.getInitialUserFollowersFromDB());//					
			windowCount++;
		}
		join.close();
	}
	
	public static void main(String[] args){
		SlidingQueryProcessor qp=new SlidingQueryProcessor();	
		for(int i=1;i<6;i++){
			qp.evaluateQuery(i);
		}
	}
}
