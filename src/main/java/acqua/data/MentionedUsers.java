package acqua.data;



public class MentionedUsers{
	long[] ids;
	String[] names;
	int index = 0;
	
	public MentionedUsers(int n){
		ids = new long[n];
		names = new String[n];
	}
	
	public void addMentionedUser(long id, String user){
		ids[index] = id;
		names[index++] = user;
	}
	
	public long getMentionedUserId(int index){
		return ids[index];
	}
	
	public String getMentionedUserName(int index){
		return names[index];
	}
}