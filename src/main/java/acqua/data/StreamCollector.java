package acqua.data;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.TreeMap;
import java.util.regex.Pattern;

import twitter4j.JSONArray;
import twitter4j.JSONObject;
import twitter4j.conf.ConfigurationBuilder;
import acqua.config.Config;




public class StreamCollector {

	
	private HashMap<Long,Integer> userMentionCount;
	public ArrayList<HashMap<Long, Integer>> windows;
	public ArrayList<ArrayList<HashMap<Long,Integer>>> windowsWithSlideEntries;
	public ArrayList<ArrayList<HashMap<Long,Long>>> slidedWindowUsersTimeStamp;
	public static long[] monitoredIds;
	public static String[] monitorNames;
	
	public StreamCollector(){
		userMentionCount=new HashMap<Long, Integer>();
		windows= new ArrayList<HashMap<Long,Integer>>();
		windowsWithSlideEntries=new ArrayList<ArrayList<HashMap<Long,Integer>>>();
		slidedWindowUsersTimeStamp=new ArrayList<ArrayList<HashMap<Long,Long>>>();
		extractUserIds();	
	}
	
	public static void extractUserIds(){
		try {
			Class.forName("org.sqlite.JDBC");
		
			Connection c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());
			Statement stmt =  c.createStatement();
			String sql1 = "SELECT COUNT(*) AS COUNT  FROM PERSON " ;
			ResultSet rs1 = stmt.executeQuery( sql1);
			int count = rs1.getInt("COUNT");
			
			int index = 0;
			monitoredIds= new long[count];
			String sql = "SELECT  USERID FROM PERSON " ;
			
			ResultSet rs = stmt.executeQuery( sql);
			while (rs.next()){
				long userId = rs.getLong("USERID");
				monitoredIds[index] =userId;
				index++;
			}
			rs.close();
			rs1.close();
			stmt.close();
			c.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public ArrayList<ArrayList<HashMap<Long, Integer>>> extractSlides(int windowMinutesLength, int slideMinutesLength,long initialTime , long endTime ){		
		
		try{
			OutputStream    fos;
			BufferedWriter bw;
			fos=new FileOutputStream(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"Debug/twitterMentionSlides.txt");
			bw=new BufferedWriter(new OutputStreamWriter(fos));
			

			//FIXME: remove the dependency to QueryProcessor
			long Wstart=Config.INSTANCE.getQueryStartingTime();
			long Sstart=Wstart;

			Queue<HashMap<Long ,Integer>> mapOfUserMentions=new LinkedList<HashMap<Long,Integer>>();
			HashMap<Long ,Integer> slideMapOfUserMention = new 	HashMap<Long ,Integer>();	
			
			Queue<HashMap<Long ,Long>> mapOfUserMentionsTime=new LinkedList<HashMap<Long,Long>>();
			HashMap<Long ,Long> slideMapOfUserMentionTime = new 	HashMap<Long ,Long>();	
			
			
			Queue<Long> slideStarts=new LinkedList<Long>();

			HashMap<Long,Long> currentWindowUsersTimestamp=new HashMap<Long, Long>();
			
			TreeMap <Long,Long> MentionsFromDB = getMentions (initialTime , endTime);//( 1359671795755l,1359673198967l );

			Iterator<Long> it = MentionsFromDB.keySet().iterator();
			
			while(it.hasNext()){//iterating through each tweet in the stream to put it in the right slide of the right window
				
				long timeStamp = it.next();
				long current = timeStamp;
				if (current-Wstart< windowMinutesLength*1000){//checking window boundaries => iterating over windows

					if (current - Sstart < slideMinutesLength*1000){//checking slide boundaries=>iterating over slides
						
						
							long mentionedUser = MentionsFromDB.get(timeStamp);
							int p=0;
							for(p=0;p<monitoredIds.length;p++)
							{
								if(monitoredIds[p]==mentionedUser)
									break;
							}
							if(p==monitoredIds.length) continue;
							Integer x = userMentionCount.get(mentionedUser);
							if(x==null)
								userMentionCount.put(mentionedUser, 1);
							else
								userMentionCount.put(mentionedUser, x+1);
							
							Object numberOfMentions = slideMapOfUserMention.get(mentionedUser);
							if(numberOfMentions!=null){
								slideMapOfUserMention.put(mentionedUser,Integer.parseInt(numberOfMentions.toString())+1);
							}else{
								slideMapOfUserMention.put(mentionedUser,1);
							}
							slideMapOfUserMentionTime.put(mentionedUser, current);
						
					}
					else{//end of current slide. adding the current slide and setting varaibles for the next slide
						bw.write(Sstart+" to "+current+" slide"+slideMapOfUserMention.toString()+"\n");
						bw.write("time : "+slideMapOfUserMentionTime.toString()+"\n");
						Sstart+=Config.INSTANCE.getQueryWindowSlide()*1000;//setting the start time of the next slide
						slideStarts.add(Sstart);//adding the slides start time for sliding windows
						mapOfUserMentions.add((HashMap<Long, Integer>)slideMapOfUserMention.clone());//adding the copy of current slide to the current window	
						mapOfUserMentionsTime.add((HashMap<Long, Long>)slideMapOfUserMentionTime.clone());//adding the copy of current slide to the current window
						slideMapOfUserMention.clear();//clearing the current slide to be filled again from stream
						slideMapOfUserMentionTime.clear();//clearing the current slide to be filled again from stream
						continue;
					}

				}else
				{//end of current window . adding the current window and setting variables for the next window 
					Wstart=slideStarts.poll();//setting the start time of the next window
					//System.out.println(mapOfUserMentions.toString());	
					mapOfUserMentions.add((HashMap<Long, Integer>)slideMapOfUserMention.clone());//adding the current slide to the slides of the current window
					mapOfUserMentionsTime.add((HashMap<Long, Long>)slideMapOfUserMentionTime.clone());//adding the current slide to the slides of the current window
					bw.write(Wstart+" TO "+current +" Window : "+mapOfUserMentions.toString()+"\n");
					bw.write("time : "+mapOfUserMentionsTime.toString()+"\n");
					windowsWithSlideEntries.add(new ArrayList<HashMap<Long,Integer>>(mapOfUserMentions));//adding the current window to the list of windows with slided entries						
					slidedWindowUsersTimeStamp.add(new ArrayList<HashMap<Long,Long>>(mapOfUserMentionsTime));//adding the list of user-entrance-timestamp for current window
					HashMap<Long,Integer> evictedUsers = mapOfUserMentions.poll();//evict the first slide from the slides of current window
					HashMap<Long,Integer> partialLastSlideEntries = ((LinkedList<HashMap<Long,Integer>>)mapOfUserMentions).removeLast();//to remove the partially added slide because it will be added fully in the next iteration
					HashMap<Long,Long> evictedUsersTime = mapOfUserMentionsTime.poll();//evict the first slide from the slides of current window
					HashMap<Long,Long> partialLastSlideEntriesTime = ((LinkedList<HashMap<Long,Long>>)mapOfUserMentionsTime).removeLast();//to remove the partially added slide because it will be added fully in the next iteration
					//evicted users should be evicted from the list of user-entrance-timestamps for the current window to be used for the next sliding window
					Iterator<Long> evictedUserIt=evictedUsers.keySet().iterator();
					while(evictedUserIt.hasNext()){
						long euid=evictedUserIt.next();
						boolean flage=false;
						for(HashMap<Long,Integer> mapOfUserMentionsSlide : mapOfUserMentions ){
							if (mapOfUserMentionsSlide.get(euid)!=null)//iterate through all slides
							{
								flage=true;
								break;
							}
						}
						if(!flage) currentWindowUsersTimestamp.remove(euid);
					}
						
					/*Iterator<Long> currentWindowUsersTimestampIt=currentWindowUsersTimestamp.keySet().iterator();
					while(currentWindowUsersTimestampIt.hasNext()){
						Long next = currentWindowUsersTimestampIt.next();
						if(partialLastSlideEntries.get(next)==null)
						currentWindowUsersTimestamp.remove(next);
					}*/
										
					continue;
				}
			}
			mapOfUserMentions.add((HashMap<Long, Integer>)slideMapOfUserMention.clone());
			windowsWithSlideEntries.add(new ArrayList<HashMap<Long,Integer>>(mapOfUserMentions));						
			//slidedWindowUsersTimeStamp.add((HashMap<Long,Long>)currentWindowUsersTimestamp.clone());
			mapOfUserMentionsTime.add((HashMap<Long, Long>)slideMapOfUserMentionTime.clone());
			slidedWindowUsersTimeStamp.add(new ArrayList<HashMap<Long,Long>>(mapOfUserMentionsTime));						
			//slidedWindowUsersTimeStamp.add((HashMap<Long,Long>)currentWindowUsersTimestamp.clone());
			bw.flush();
			bw.close();
			return windowsWithSlideEntries;

		}catch(Exception e){e.printStackTrace(); return null;}

	}


	public ArrayList<HashMap<Long,Integer>> aggregateSildedWindowsUser(){
		
		ArrayList<HashMap<Long,Integer>> slidedWindows=new ArrayList<HashMap<Long,Integer>>();
		for(int i=0;i<windowsWithSlideEntries.size();i++){
			ArrayList<HashMap<Long,Integer>> tempSplittedWindow = windowsWithSlideEntries.get(i);
			HashMap<Long,Integer> WindowUserMention=new HashMap<Long, Integer>();
			for(int j=0;j<tempSplittedWindow.size();j++){//per slide
				HashMap<Long,Integer> slideUsers=tempSplittedWindow.get(j);
				Iterator<Long> slideuserit=slideUsers.keySet().iterator();
				while(slideuserit.hasNext()){
					long slideUserName=slideuserit.next();
					Integer windowusercount= WindowUserMention.get(slideUserName);
					if(windowusercount==null){
						WindowUserMention.put(slideUserName, slideUsers.get(slideUserName));
					}else{
						WindowUserMention.put(slideUserName, slideUsers.get(slideUserName)+windowusercount);
					}
				}				
			}
			slidedWindows.add(WindowUserMention);
		}
		try{
			HashMap<Long,Integer> c= TwitterFollowerCollector.getchangecount();
			BufferedWriter bw1=new BufferedWriter(new FileWriter(new File(Config.INSTANCE.getProjectPath()+Config.INSTANCE.getDatasetFolder()+"Debug/userMentionCount.csv")));
		Iterator<Long> it1= userMentionCount.keySet().iterator();
		while(it1.hasNext()){
			Long idtemp=it1.next();
			bw1.write(idtemp+","+userMentionCount.get(idtemp)+","+c.get(idtemp)+"\n");
		}
		bw1.flush();
		bw1.close();
		}catch(Exception e){}
		
		
		return slidedWindows;
	}
	
	public ArrayList<HashMap<Long,Long>> aggregateSildedWindowsUserTime(){
		
		ArrayList<HashMap<Long,Long>> slidedWindowsTime=new ArrayList<HashMap<Long,Long>>();
		for(int i=0;i<slidedWindowUsersTimeStamp.size();i++){
			ArrayList<HashMap<Long,Long>> tempSplittedWindowTime = slidedWindowUsersTimeStamp.get(i);
			HashMap<Long,Long> WindowUserMentionTime=new HashMap<Long, Long>();
			for(int j=0;j<tempSplittedWindowTime.size();j++){//per slide
				HashMap<Long,Long> slideUsersTime=tempSplittedWindowTime.get(j);
				Iterator<Long> slideuserit=slideUsersTime.keySet().iterator();
				while(slideuserit.hasNext()){
					long slideUserName=slideuserit.next();
					Long windowusertime= WindowUserMentionTime.get(slideUserName);
					WindowUserMentionTime.put(slideUserName, slideUsersTime.get(slideUserName));					
				}				
			}
			slidedWindowsTime.add(WindowUserMentionTime);
		}
		return slidedWindowsTime;
	}

	private TreeMap <Long,Long> getMentions(long initialTime , long endTime){
		
		TreeMap <Long,Long> result = new TreeMap <Long,Long>();
		
		try {
			
			Class.forName("org.sqlite.JDBC");
			Connection c = DriverManager.getConnection(Config.INSTANCE.getDatasetDb());
			Statement stmt =  c.createStatement();
			String sql = "SELECT TIMESTAMP , USERID FROM MENTIONS POST WHERE TIMESTAMP >= " + initialTime + " AND TIMESTAMP <= " + endTime + " ORDER BY TIMESTAMP ASC" ;
			
			ResultSet rs = stmt.executeQuery( sql);
			while ( rs.next() ) {
				long timestamp = rs.getLong("TIMESTAMP");
				long userId = rs.getLong("USERID");
				result.put(timestamp, userId);
			}
			rs.close();
			stmt.close();	
		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}
	

}
