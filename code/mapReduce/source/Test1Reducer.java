import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Test1Reducer extends Reducer<Text, Text, Text, Text> {
	
	public int engagement = 0, potentialReach = 0, potentialImpressions = 0;
	public  float responseRate = 0;

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {


		// First row will give all final values
		// second row onwards will the rows which contributed to the above values
		
		// Calculate first rows
		int mentionRepliedTo = 0, totalMention = 0;
		
		//ArrayList<TweetData> tweetData = new ArrayList<TweetData>();
		Hashtable<String, Integer> perUserFavCount = new Hashtable<String, Integer>();
		
		for (Text val : values) {
			String line = val.toString();
			int mentions =0, replies = 0, favorites = 0, retweets = 0;
			String user = "";
			int umention =0, uFavCount = 0, mentionReply = 0, tMention = 0;
			ArrayList<String> parts = new ArrayList<String> (Arrays.asList(line.split("[,]+")));
			replies += Integer.parseInt(parts.get(0));
			retweets += Integer.parseInt(parts.get(1));
			mentions += Integer.parseInt(parts.get(2));
			favorites += Integer.parseInt(parts.get(3));
			user = parts.get(4);
			umention = Integer.parseInt(parts.get(5));
			uFavCount = Integer.parseInt(parts.get(6));
			mentionReply = Integer.parseInt(parts.get(7));
			tMention = Integer.parseInt(parts.get(8));
			//TweetData t = new TweetData();
			//t.setTweetData(replies, retweets,mentions,favorites,user,umention,uFavCount,mentionRepliedTo,totalMention);
			//tweetData.add(t);
			// engagement and potentialImpressions calculations
			engagement += replies + retweets + mentions + favorites;
			potentialImpressions += umention + uFavCount;
			// responseRate calculation
			mentionRepliedTo += mentionReply;
			totalMention += tMention;
			
			// potentialReach calculation
			if (perUserFavCount.containsKey(user)) {
				int temp = perUserFavCount.get(user);
				perUserFavCount.put(user, Math.max(uFavCount,temp));
			} else {
				perUserFavCount.put(user, uFavCount);
			}
			context.write(new Text (" "), val);
		}
		
		
		potentialReach = perUserFavCount.size();
		Collection<Integer> c = perUserFavCount.values();
		Iterator<Integer> i = c.iterator();
		while (i.hasNext()) {
			potentialReach +=  i.next();
		}
		
		if (totalMention != 0) {
			responseRate = (float) (mentionRepliedTo / totalMention);
		} else {
			responseRate = 0;
		}
		
		context.write(new Text(" "), new Text("Engagement = " + engagement
				+ " , Potential Reach = " + potentialReach
				+ " , Potential Impressions = " + potentialImpressions
				+ " , Response Rate = " + responseRate));

//		for (Text val : values) {
//			context.write(new Text (" "), val);
//		}
	}
	
	
	public class TweetData {
		public int mentions =0, replies = 0, favorites = 0, retweets = 0;
		public String user = "";
		public int umention =0, uFavCount = 0, mentionRepliedTo = 0, totalMention = 0;
		
		public void setTweetData(int v1, int v2, int v3, int v4, String v5, int v6, int v7, int v8, int v9) {

			replies = v1;
			retweets = v2;
			mentions = v3;
			favorites = v4;
			user = v5;
			umention = v6;
			uFavCount = v7;
			mentionRepliedTo = v8;
			totalMention = v9;
		}
	}
	
	public void previousCode(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text val : values) {
			String line = val.toString();
			ArrayList<String> parts = new ArrayList<String> (Arrays.asList(line.split("[|]+")));
			for(int i=0;i<parts.size();i++) {
				if (parts.get(i).compareToIgnoreCase("[\\s]+") != 0) {
					//context.write(new Text((i+1)+""), new Text(parts.get(i)));					
				}
			}
		}
	}

}

