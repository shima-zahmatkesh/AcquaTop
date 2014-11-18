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


import twitter4j.JSONArray;
import twitter4j.JSONObject;


public class ParseTweets {
	public static void main(String[] args)
	{
		try{
			OutputStream    fos;
			BufferedWriter bw;
			fos=new FileOutputStream("D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/twitterMentionWindows.txt");
			bw=new BufferedWriter(new OutputStreamWriter(fos));
			InputStream    fis;
			BufferedReader br;
			
			long start=new Long("1416074389529");
			int windowSize=30;
			
			fis = new FileInputStream("D:/softwareData/git-clone-https---soheilade-bitbucket.org-soheilade-acqua.git/acquaProj/followerSnapshotsFile2.txt");
			br = new BufferedReader(new InputStreamReader(fis));
			String line = br.readLine();
			
			ArrayList<HashMap> windows=new ArrayList<HashMap>();
			HashMap<String ,Integer> mapOfUserMentions=new HashMap<String, Integer>();
			
			
			while(line!=null){
				JSONObject jsnobject = new JSONObject(line);
				Object timeStamp = jsnobject.get("timestamp_ms");
				long current = Long.parseLong(timeStamp.toString());
				if (current-start< windowSize*1000){
					JSONObject jsonEntities =(JSONObject)jsnobject.get("entities");
					JSONArray jsonMentionArray=jsonEntities.getJSONArray("user_mentions");
					 for (int i = 0; i < jsonMentionArray.length(); i++) {
					        JSONObject explrObject = jsonMentionArray.getJSONObject(i);
					        String mentionedUser = explrObject.get("screen_name").toString();
					        Object numberOfMentions = mapOfUserMentions.get(mentionedUser);
					        if(numberOfMentions!=null){
					        	mapOfUserMentions.put(mentionedUser,Integer.parseInt(numberOfMentions.toString())+1);
					        }else{
					        	mapOfUserMentions.put(mentionedUser,1);
					        }
					}
				}else
					{
						start=current;
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
