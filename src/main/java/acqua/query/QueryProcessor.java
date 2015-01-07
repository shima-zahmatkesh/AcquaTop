package acqua.query;
import acqua.*;
import acqua.config.Config;
import acqua.data.TwitterStreamCollector;
import acqua.query.join.JoinOperator;
import acqua.query.join.bkg1.DWJoinOperator;
import acqua.query.join.bkg1.OracleJoinOperator;
import acqua.query.join.bkg1.SmartJoin;
import acqua.query.join.bkg1.BaselineJoinOperator;
import acqua.query.join.bkg1.RandomCacheUpdateJoin;
import acqua.query.join.bkg2.OracleDoubleJoinOperator;
import acqua.query.join.bkg2.DoubleBkgJoinOperator;

public class QueryProcessor {
	JoinOperator join;
	TwitterStreamCollector tsc;	
	//HashMap<Long, Integer> initialCache;
//	public static long start=1416244306470L;//select min(TIMESTAMP) + 30000 from BKG 
//	public static int windowSize=60;
	public QueryProcessor(){//class JoinOperator){
		//updateBudget and join should be initiated
		//join=new 
		
		tsc= new TwitterStreamCollector();
		tsc.extractWindow(Config.INSTANCE.getQueryWindowWidth(), "D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/twitterStream.txt");		
		for (int i=0;i<tsc.windows.size();i++)
			System.out.println(tsc.windows.get(i).size());
		//initialCache = tfc.getFollowerListFromDB(start); //gets the first window
	}
	public void evaluateQuery(int joinType){
		if(joinType==1)
			join=new OracleJoinOperator();
		if(joinType==2)
			join=new DWJoinOperator();
		if(joinType==3)
			join=new BaselineJoinOperator(10);//update budget of 10
		if(joinType==4)
			join=new RandomCacheUpdateJoin(10);
		if(joinType==5)
			join=new SmartJoin(10);
		if(joinType==6)
			join=new DoubleBkgJoinOperator(10);
		if(joinType==7)
			join=new OracleDoubleJoinOperator();
		long time=Config.INSTANCE.getQueryStartingTime();
		int windowCount=0;
		while(windowCount<100){
			time = time + Config.INSTANCE.getQueryWindowWidth()*1000;	
			//System.out.println(tsc.windows.get(windowCount).size());
			join.process(time,tsc.windows.get(windowCount));//TwitterFollowerCollector.getInitialUserFollowersFromDB());//					
			windowCount++;
		}
		join.close();
	}
	
	public static void main(String[] args){
		QueryProcessor qp=new QueryProcessor();	
		for(int i=1;i<6;i++){
			qp.evaluateQuery(i);
		}
	}
}
