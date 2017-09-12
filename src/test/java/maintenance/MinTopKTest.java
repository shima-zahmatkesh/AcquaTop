package maintenance;

import java.util.HashMap;
import java.util.TreeMap;

import acqua.maintenance.MinTopK;

public class MinTopKTest {
	
	public MinTopK minTopK = new MinTopK();
	
	public void populatefollowerReplica(){
		minTopK.followerReplica.put(101l , 1000);
		minTopK.followerReplica.put(102l , 700);
		minTopK.followerReplica.put(103l , 900);
		minTopK.followerReplica.put(104l , 800);
		minTopK.followerReplica.put(105l , 1200);
		minTopK.followerReplica.put(106l , 500);
		minTopK.followerReplica.put(107l , 700);
		minTopK.followerReplica.put(108l , 900);
		minTopK.followerReplica.put(109l , 1100);
		minTopK.followerReplica.put(110l , 1400);
		minTopK.followerReplica.put(111l , 600);
		minTopK.followerReplica.put(112l , 500);
		minTopK.followerReplica.put(113l , 1200);
		minTopK.followerReplica.put(114l , 600);
		minTopK.followerReplica.put(115l , 1100);
	}


	
	public void populateSlidedWindow(){
		minTopK.slidedwindows.add(0 , null);
		minTopK.slidedwindows.add(1 , null);
		minTopK.slidedwindows.add(2 , null);
		
		TreeMap<Long, Integer> followers1 =  new TreeMap<Long, Integer>();
		followers1.put(101l, 200);
		followers1.put(102l, 200);
		followers1.put(103l, 400);
		followers1.put(104l, 300);
		followers1.put(105l, 200);
		followers1.put(106l, 400);
		followers1.put(107l, 300);
		followers1.put(108l, 600);
		followers1.put(109l, 200);
		minTopK.slidedwindows.add(3 , followers1);
		
		TreeMap<Long, Integer> followers2 =  new TreeMap<Long, Integer>();
		
		followers2.put(104l, 300);
		followers2.put(105l, 200);
		followers2.put(106l, 400);
		followers2.put(107l, 300);
		followers2.put(108l, 600);
		followers2.put(109l, 200);
		followers2.put(110l, 100);
		followers2.put(111l, 300);
		followers2.put(112l, 400);
		minTopK.slidedwindows.add(4 , followers2);
		
		TreeMap<Long, Integer> followers3 =  new TreeMap<Long, Integer>();
		
		followers3.put(107l, 300);
		followers3.put(108l, 600);
		followers3.put(109l, 200);
		followers3.put(110l, 100);
		followers3.put(111l, 300);
		followers3.put(112l, 400);
		followers3.put(113l, 400);
		followers3.put(114l, 200);
		followers3.put(115l, 100);
		minTopK.slidedwindows.add(5 , followers3);
		
		
		
	}
	
	public void populateSlidedWindowTime(){
		
		minTopK.slidedwindowsTime.add(0 , null);
		minTopK.slidedwindowsTime.add(1 , null);
		minTopK.slidedwindowsTime.add(2 , null);
		
		TreeMap<Long, Long> followers1 =  new TreeMap<Long, Long>();
		followers1.put(101l, 10l);
		followers1.put(102l, 11l);
		followers1.put(103l, 12l);
		followers1.put(104l, 13l);
		followers1.put(105l, 14l);
		followers1.put(106l, 15l);
		followers1.put(107l, 16l);
		followers1.put(108l, 17l);
		followers1.put(109l, 18l);
		minTopK.slidedwindowsTime.add(3 , followers1);
		
		TreeMap<Long, Long> followers2 =  new TreeMap<Long, Long>();
		
		followers2.put(104l, 13l);
		followers2.put(105l, 14l);
		followers2.put(106l, 15l);
		followers2.put(107l, 16l);
		followers2.put(108l, 17l);
		followers2.put(109l, 18l);
		followers2.put(110l, 19l);
		followers2.put(111l, 20l);
		followers2.put(112l, 21l);
		minTopK.slidedwindowsTime.add(4 , followers2);
		
		TreeMap<Long, Long> followers3 =  new TreeMap<Long, Long>();
		
		followers3.put(107l, 16l);
		followers3.put(108l, 17l);
		followers3.put(109l, 18l);
		followers3.put(110l, 19l);
		followers3.put(111l, 20l);
		followers3.put(112l, 21l);
		followers3.put(113l, 22l);
		followers3.put(114l, 23l);
		followers3.put(115l, 24l);
		minTopK.slidedwindowsTime.add(5 , followers3);
		
		
		
	}
	
	public void populateBackgrounChanges(){
		minTopK.Backgroundchanges.add(0 , null);
		minTopK.Backgroundchanges.add(1 , null);
		minTopK.Backgroundchanges.add(2 , null);
		TreeMap<Long, Integer> followers1 =  new TreeMap<Long, Integer>();
		followers1.put(102l, 1200);
		minTopK.Backgroundchanges.add(3 , followers1);
		
		TreeMap<Long, Integer> followers2 =  new TreeMap<Long, Integer>();
		followers2.put(105l, 900);
		minTopK.Backgroundchanges.add(4 , followers2);
		
		TreeMap<Long, Integer> followers3 =  new TreeMap<Long, Integer>();
		followers3.put(113l, 800);
		minTopK.Backgroundchanges.add(5 , followers3);
		
		
	}
	
	public static void main(String[] args){
		
		MinTopKTest test = new MinTopKTest();
//		test.populatefollowerReplica();
//		test.populateSlidedWindow();
//		test.populateSlidedWindowTime();
//		test.populateBackgrounChanges();
//		
		test.populatefollowerReplica2();
		test.populateSlidedWindow2();
		test.populateSlidedWindowTime2();
		test.populateBackgrounChanges2();
		
		test.minTopK.evaluateQuery();
//		

	}
	
	
	public void populatefollowerReplica2(){
		minTopK.followerReplica.put(101l , 1000);
		minTopK.followerReplica.put(102l , 700);
		minTopK.followerReplica.put(103l , 900);
		minTopK.followerReplica.put(104l , 800);
		minTopK.followerReplica.put(105l , 1000);
		minTopK.followerReplica.put(106l , 1000);
		minTopK.followerReplica.put(107l , 900);
		minTopK.followerReplica.put(108l , 900);
		minTopK.followerReplica.put(109l , 800);
		minTopK.followerReplica.put(110l , 1400);
		minTopK.followerReplica.put(111l , 600);
		minTopK.followerReplica.put(112l , 1000);
		minTopK.followerReplica.put(113l , 800);
		minTopK.followerReplica.put(114l , 1200);
		minTopK.followerReplica.put(115l , 1500);
		minTopK.followerReplica.put(116l , 1000);
		minTopK.followerReplica.put(117l , 1000);
		minTopK.followerReplica.put(118l , 1500);
		minTopK.followerReplica.put(119l , 1500);
		minTopK.followerReplica.put(120l , 1500);
		minTopK.followerReplica.put(121l , 1000);
	}


	
	public void populateSlidedWindow2(){
		minTopK.slidedwindows.add(0 , null);
		minTopK.slidedwindows.add(1 , null);
		minTopK.slidedwindows.add(2 , null);
		
		TreeMap<Long, Integer> followers1 =  new TreeMap<Long, Integer>();
		followers1.put(101l, 400);
		followers1.put(102l, 500);
		followers1.put(103l, 400);
		followers1.put(104l, 200);
		followers1.put(105l, 100);
		followers1.put(106l, 500);
		followers1.put(107l, 500);
		followers1.put(108l, 300);
		followers1.put(109l, 200);
		minTopK.slidedwindows.add(3 , followers1);
		
		TreeMap<Long, Integer> followers2 =  new TreeMap<Long, Integer>();
		
		followers2.put(104l, 200);
		followers2.put(105l, 100);
		followers2.put(106l, 500);
		followers2.put(107l, 500);
		followers2.put(108l, 300);
		followers2.put(109l, 200);
		followers2.put(110l, 100);
		followers2.put(111l, 700);
		followers2.put(112l, 700);
		minTopK.slidedwindows.add(4 , followers2);
		
		TreeMap<Long, Integer> followers3 =  new TreeMap<Long, Integer>();
		
		followers3.put(107l, 500);
		followers3.put(108l, 100);
		followers3.put(109l, 200);
		followers3.put(110l, 100);
		followers3.put(111l, 700);
		followers3.put(112l, 700);
		followers3.put(113l, 800);
		followers3.put(114l, 200);
		followers3.put(115l, 100);
		minTopK.slidedwindows.add(5 , followers3);
		
		TreeMap<Long, Integer> followers4 =  new TreeMap<Long, Integer>();
		
		
		followers4.put(110l, 100);
		followers4.put(111l, 700);
		followers4.put(112l, 700);
		followers4.put(113l, 800);
		followers4.put(114l, 200);
		followers4.put(115l, 100);
		followers4.put(116l, 300);
		followers4.put(117l, 200);
		followers4.put(118l, 200);
		minTopK.slidedwindows.add(6 , followers4);
		
		TreeMap<Long, Integer> followers5 =  new TreeMap<Long, Integer>();
		
		followers5.put(113l, 800);
		followers5.put(114l, 200);
		followers5.put(115l, 100);
		followers5.put(116l, 300);
		followers5.put(117l, 200);
		followers5.put(118l, 200);
		followers5.put(119l, 400);
		followers5.put(120l, 300);
		followers5.put(121l, 500);
		minTopK.slidedwindows.add(7 , followers5);
		
		
		
	}
	
	public void populateSlidedWindowTime2(){
		
		minTopK.slidedwindowsTime.add(0 , null);
		minTopK.slidedwindowsTime.add(1 , null);
		minTopK.slidedwindowsTime.add(2 , null);
		
		TreeMap<Long, Long> followers1 =  new TreeMap<Long, Long>();
		followers1.put(101l, 10l);
		followers1.put(102l, 11l);
		followers1.put(103l, 12l);
		followers1.put(104l, 13l);
		followers1.put(105l, 14l);
		followers1.put(106l, 15l);
		followers1.put(107l, 16l);
		followers1.put(108l, 17l);
		followers1.put(109l, 18l);
		minTopK.slidedwindowsTime.add(3 , followers1);
		
		TreeMap<Long, Long> followers2 =  new TreeMap<Long, Long>();
		
		followers2.put(104l, 13l);
		followers2.put(105l, 14l);
		followers2.put(106l, 15l);
		followers2.put(107l, 16l);
		followers2.put(108l, 17l);
		followers2.put(109l, 18l);
		followers2.put(110l, 19l);
		followers2.put(111l, 20l);
		followers2.put(112l, 21l);
		minTopK.slidedwindowsTime.add(4 , followers2);
		
		TreeMap<Long, Long> followers3 =  new TreeMap<Long, Long>();
		
		followers3.put(107l, 16l);
		followers3.put(108l, 17l);
		followers3.put(109l, 18l);
		followers3.put(110l, 19l);
		followers3.put(111l, 20l);
		followers3.put(112l, 21l);
		followers3.put(113l, 22l);
		followers3.put(114l, 23l);
		followers3.put(115l, 24l);
		minTopK.slidedwindowsTime.add(5 , followers3);
		
		
		TreeMap<Long, Long> followers4 =  new TreeMap<Long, Long>();
		
		
		followers4.put(110l, 19l);
		followers4.put(111l, 20l);
		followers4.put(112l, 21l);
		followers4.put(113l, 22l);
		followers4.put(114l, 23l);
		followers4.put(115l, 24l);
		followers4.put(116l, 25l);
		followers4.put(117l, 26l);
		followers4.put(118l, 27l);
		minTopK.slidedwindowsTime.add(6 , followers4);
		
		
		TreeMap<Long, Long> followers5 =  new TreeMap<Long, Long>();
		
		followers5.put(113l, 22l);
		followers5.put(114l, 23l);
		followers5.put(115l, 24l);
		followers5.put(116l, 25l);
		followers5.put(117l, 26l);
		followers5.put(118l, 27l);
		followers5.put(119l, 28l);
		followers5.put(120l, 29l);
		followers5.put(121l, 30l);
		minTopK.slidedwindowsTime.add(7 , followers5);
		
		
		
	}
	
	public void populateBackgrounChanges2(){
		minTopK.Backgroundchanges.add(0 , null);
		minTopK.Backgroundchanges.add(1 , null);
		minTopK.Backgroundchanges.add(2 , null);
		
		TreeMap<Long, Integer> followers1 =  new TreeMap<Long, Integer>();
		followers1.put(105l, 1600);
		minTopK.Backgroundchanges.add(3 , followers1);
		
		TreeMap<Long, Integer> followers2 =  new TreeMap<Long, Integer>();
		followers2.put(112l, 400);
		minTopK.Backgroundchanges.add(4 , followers2);
		
		TreeMap<Long, Integer> followers3 =  new TreeMap<Long, Integer>();
		//followers3.put(115l, 1700);
		followers3.put(112l, 900);
		minTopK.Backgroundchanges.add(5 , followers3);
		
		TreeMap<Long, Integer> followers4 =  new TreeMap<Long, Integer>();
		followers4.put(118l, 1900);
		minTopK.Backgroundchanges.add(6 , followers4);
		
		TreeMap<Long, Integer> followers5 =  new TreeMap<Long, Integer>();
		followers5.put(119l, 1100);
		minTopK.Backgroundchanges.add(7 , followers5);
		
		
		
	}
	
}
