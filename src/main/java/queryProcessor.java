import java.awt.image.ReplicateScaleFilter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.xerces.impl.xpath.regex.REUtil;


public class queryProcessor {
	JoinOperator join;
	twitterStreamCollector tsc;	
	//HashMap<Long, Integer> initialCache;
	public static long start=1416244306470L;//select min(TIMESTAMP) + 30000 from BKG 
	public static int windowSize=30;
	public queryProcessor(){//class JoinOperator){
		//updateBudget and join should be initiated
		//join=new 
		
		tsc= new twitterStreamCollector();
		tsc.extractWindow(windowSize, "D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/twitterStream.txt");		
		//initialCache = tfc.getFollowerListFromDB(start); //gets the first window
	}
	public void evaluateQuery(int joinType){
		if(joinType==1)
			join=new OracleJoinOperator();
		if(joinType==2)
			join=new DWJoinOperator();
		if(joinType==3)
			join=new BaselineJoinOperator(10);//update budget of 10
		long time=queryProcessor.start;
		int windowCount=0;
		while(windowCount<30){
			time = time + 30000;
			//System.out.println(time +" "+windowCount+" "+queryProcessor.windowSize);
			join.process(time,tsc.windows.get(windowCount));
			windowCount++;
		}
		join.close();
	}
	
	
	public static void main(String[] args){
		queryProcessor qp=new queryProcessor();	
		qp.evaluateQuery(1);
		
		
	}
}
