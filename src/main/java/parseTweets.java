import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

//import com.fasterxml.jackson.databind.ObjectMapper;

import twitter4j.JSONArray;
import twitter4j.JSONObject;


public class parseTweets {
	public static void main(String[] args)
	{
		try{
			OutputStream    fos;
			BufferedWriter bw;
			fos=new FileOutputStream("G:/acquaProj/twitterMentionWindows.txt");
			bw=new BufferedWriter(new OutputStreamWriter(fos));
			InputStream    fis;
			BufferedReader br;
			
			long start=new Long("1416074389529");
			int windowSize=5;
			
			fis = new FileInputStream("G:/acquaProj/twitterStream.txt");
			br = new BufferedReader(new InputStreamReader(fis));
			String line = br.readLine();
			
			ArrayList<HashMap> windows=new ArrayList<HashMap>();
			HashMap<String ,Integer> mapOfUserMentions=new HashMap<String, Integer>();
			
			
			while(line!=null){
				JSONObject jsnobject = new JSONObject(line);
				Object timeStamp = jsnobject.get("timestamp_ms");
				long Current = Long.parseLong(timeStamp.toString());
				if (Current-start< windowSize*6000){
					JSONObject jsonEntities =(JSONObject)jsnobject.get("entities");
					JSONArray jsonMentionArray=jsonEntities.getJSONArray("user_mentions");
					 for (int i = 0; i < jsonMentionArray.length(); i++) {
					        JSONObject explrObject = jsonMentionArray.getJSONObject(i);
					        String MentionedUser = explrObject.get("screen_name").toString();
					        Object numberOfMentions = mapOfUserMentions.get(MentionedUser);
					        if(numberOfMentions!=null){
					        	mapOfUserMentions.put(MentionedUser,Integer.parseInt(numberOfMentions.toString())+1);
					        }else{
					        	mapOfUserMentions.put(MentionedUser,1);
					        }
					}
				}else
					{
						start=Current;
						System.out.println(mapOfUserMentions.toString());
						bw.write(mapOfUserMentions.toString()+"\n");
						windows.add((HashMap)mapOfUserMentions.clone());						
						mapOfUserMentions.clear();
						continue;
						}
				line=br.readLine();
			}
			br.close();
			bw.flush();
			bw.close();
			
		}catch(Exception e){e.printStackTrace();}
		
	}

}
