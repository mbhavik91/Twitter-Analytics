import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Hashtable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;



public class Test1Mapper extends Mapper<LongWritable, Text, Text, Text> implements JobConfigurable {
	
	/*public ArrayList<String> ipHandles = new ArrayList<String> ();
	public ArrayList<String> ipTags = new ArrayList<String> ();*/
	public ArrayList<String> ipHandles;
	public ArrayList<String> ipTags;

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		Tweet tweet = new Tweet();
		String line = value.toString();
		String umk = "", umval = "";
		if (line.compareTo("") != 0) {
			ArrayList<String> parts = new ArrayList<String>(Arrays.asList(line.split("[|]+")));
			for (int i = 0; i < parts.size()-1; i++) {
				//if (parts.get(i).compareToIgnoreCase("[\\s]+") != 0) {
				if (parts.get(i).trim().length() != 0) {
					ArrayList<String> fields = new ArrayList<String>(Arrays.asList(parts.get(i).split("[=]+")));
					//String k = fields.get(0).replaceAll("\\s+", "");
					String k = fields.get(0).trim();
					k = k.replaceAll("[\\s]+", "");
					String val = "";
					try {
						val = fields.get(1);
					} catch (Exception e) {
						val = "###";
						e.printStackTrace();
					}
					if (k.compareToIgnoreCase("text") != 0) {
						val = val.replaceAll("[\\s]+", "");
						val.trim();
					}
					if ((k.compareToIgnoreCase("hashtags") == 0) || (k.compareToIgnoreCase("user_mentions") == 0)) {
						if (k.compareToIgnoreCase("hashtags") == 0) {
							tweet.setHashtags(val);
						}
						else {
							//if (k.compareToIgnoreCase("user_mentions") == 0) {
							tweet.setUserMentions(val);
							umk = k;
							umval = val;
						}
					}
					else {
						tweet.setValues(k,val);
					}

				}
			}
			
			// calculate engagement metrics
			ArrayList<Integer> engagement = getEngagement(tweet);
			// calculate potential reach metrics
			// final values = potentialUser, 0/1, favCount
			// if potentialUser = None, then dont consider this, as next 2 values will be 0 and 0
			// if potentialUser != None, then getPotentialReach calculation will be used differently
			// to calcualte potentialReach and potentialImpressions
			ArrayList<Integer> potentialReach = getPotentialReach(tweet);
			String potentialUser = "";
			if (potentialReach.get(0) == 1) {
				potentialUser = tweet.uscreen_name;
			} else {
				potentialUser = "None";
			}
			
			
			// calculate responseRate metrics
			ArrayList<Integer> responseRate = getResponseRate(tweet);
			
			
			
			String val = "";
			for (int i=0;i<engagement.size();i++){
				val = val+engagement.get(i)+",";
				//val.concat(",");
			}
			
			val += potentialUser + ",";
			for(int i=0;i<potentialReach.size();i++) {
				val = val + potentialReach.get(i)+",";
			}
			
			for(int i=0;i<responseRate.size();i++) {
				val = val + responseRate.get(i)+",";
			}
			
			/*context.write(new Text(1 + ""), new Text(val
					+ "__tweet.user_mentions=" + tweet.user_mentions.toString()
					+ "_tweet.retweet_Flag=" + tweet.retweet_flag
					+ "__tweet.retweetForUser=" + tweet.retweetForUser));*/
			if ((val.compareToIgnoreCase("0,0,0,0,None,0,0,0,0,") != 0) && (line.compareTo("") !=0) ) {
				context.write(new Text(1 + ""), new Text(val));
				// context.write(new Text(1+""), new Text(engagement.toString()));
			}	
		}	
		
	}
	
	// calculate responseRate metrics
	// Returns 2 values
	// 0: mentions count replied to	
	// 1: total mentions
	// Retweet count will be zero, as if the tweet is a retweet, then we will not consider it in mentions count either
	// so this has an effect of nullifying. Because whenever there is a retweet, the tweet also shows the handle 
	// in user_mentions list
	
	public ArrayList<Integer> getResponseRate(Tweet tweet) {
		ArrayList<Integer> responseRate = new ArrayList<Integer> ();
		boolean flag = false;
		String user = "";
		// find which user is mentioned in the tweet
		for (int i=0;i<ipHandles.size();i++) {
			if(tweet.user_mentions.contains(ipHandles.get(i))) {
				flag = true;
				user = ipHandles.get(i);
				break;
			}
		}
		
		// find numerator, i.e. total number of mentions replied to
		// if tweet.uscreen_name == user AND tweet.retweet_flag == false AND tweet.in_reply_to_screen_name != "###"
		// 0th value = 1  , i.e. Total no of mentions replied to
		if ((tweet.uscreen_name.compareToIgnoreCase(user) == 0) && (tweet.retweet_flag == false) && (tweet.in_reply_to_screen_name.compareToIgnoreCase("###") != 0) ) {
			responseRate.add(1);
		} else {
			responseRate.add(0);
		}
		
		// find total mentions, i.e. whether the desired user is mentioned in the tweet or not
		if (flag == true) {
			responseRate.add(1);
		} else {
			responseRate.add(0);
		}		
		return responseRate;
	}
	
	
	// Returns parameters for calculating potential reach contains 2 values
	// 0th value: 1, if user mentions the brand, else 0
	// 1st value: x, if 0th value==1, then x = no of followers of user, else x = 0
	public ArrayList<Integer> getPotentialReach(Tweet tweet) {
		ArrayList<Integer> potentialReach = new ArrayList<Integer>();
		for (int i=0;i<ipHandles.size();i++) {
			if(tweet.user_mentions.contains(ipHandles.get(i))) {
				if (!tweet.uscreen_name.equalsIgnoreCase(ipHandles.get(i))) {
					potentialReach.add(1);
				} else {
					potentialReach.add(0);
				}
			} else {
				potentialReach.add(0);
			}
		}
		
		if (potentialReach.get(0) == 1) {
			potentialReach.add(tweet.ufollowers_count);
		} else {
			potentialReach.add(0);
		}
		return potentialReach;
	}
	
	
	// Returns parameters for calculating engagement
	// contains only 4 values
	// 0: no of replies		1: no of retweets
	// 2: no of mentions	3: favorite count	
	public ArrayList<Integer> getEngagement(Tweet tweet) {
		ArrayList<Integer> engagement = new ArrayList<Integer>();
		if (ipHandles.contains(tweet.in_reply_to_screen_name)) {
			// calculate @Replies
			// if input handles (one we are searching) is the one replied to, then this is a reply
			engagement.add(1);
		} else {
			engagement.add(0);
		}
		// calculate retweet
		// Find whether the tweet is a retweet or not. if it is a retweet, then whether it is for our desired
		// user or handle
		if (tweet.retweet_flag == true) {
			if (ipHandles.contains(tweet.retweetForUser)) {
				engagement.add(Math.max(tweet.oretweet_count,tweet.uretweet_count));
			} else {
				engagement.add(0);
			}
		} else {
			engagement.add(0);
		}
		
		// find user mentions from the arrayList
		int umention = 0;
		
		for(int i=0;i<tweet.user_mentions.size();i++) {
			if (ipHandles.contains(tweet.user_mentions.get(i))) {
				umention = 1;
				break;
			} else {
				umention = 0;
			}
		}
		engagement.add(umention);
		
		
		// find favorites. if uscreen_name is in ipHandles, engagement[3] = ufavorite_count, else engagement[3] = 0
		if (ipHandles.contains(tweet.uscreen_name)) {
			engagement.add(tweet.ufavorite_count);
		} else {
			engagement.add(0);
		}			
		return engagement;
	}

	
	
	public class Tweet {
		public int ofavorite_count = 0;
		public int oretweet_count = 0;
		public int uretweet_count = 0;
		public int ofollowers_count = 0;
		public int ufollowers_count = 0;
		public int ufavorite_count= 0;
		public String text = "";
		public boolean retweet_flag = false;
		public String retweetForUser = "";
		public ArrayList<String> hashtags = new ArrayList<String>();
		public ArrayList<String> user_mentions = new ArrayList<String>();
		public String oscreen_name = "";
		public String in_reply_to_screen_name = "";
		public String uscreen_name = "";
		
		
		public void setHashtags(String htags) {
			if (htags.compareToIgnoreCase("###") == 0) {
				return;
			}
			ArrayList<String> parts = new ArrayList<String>(Arrays.asList(htags.split("[,]+")));
			for(int i=0;i<parts.size();i++) {
				String temp = parts.get(i).trim();
				if (temp.length() != 0) {
					hashtags.add(temp);
				}
			}
			
		}
		
		public void setUserMentions(String umentions) {
			if (umentions.compareToIgnoreCase("###") == 0) {
				return;
			}
			ArrayList<String> parts = new ArrayList<String>(Arrays.asList(umentions.split("[,]+")));
			for(int i=0;i<parts.size();i++) {
				String temp = parts.get(i).trim();
				if (temp.length() != 0) {
					user_mentions.add(temp);
				}
			}
		}
		
		public void setValues(String fieldName, String val) {
			if (fieldName.compareToIgnoreCase("ofavorite_count") == 0) {
				if (val.compareToIgnoreCase("###") != 0) {
					ofavorite_count = Integer.parseInt(val);
				}
			}
			else if (fieldName.compareToIgnoreCase("oretweet_count") == 0) {
				if (val.compareToIgnoreCase("###") != 0) {
					oretweet_count = Integer.parseInt(val);
				}
			}
			else if (fieldName.compareToIgnoreCase("uretweet_count") == 0) {
				if (val.compareToIgnoreCase("###") != 0) {
					uretweet_count = Integer.parseInt(val);
				}
			}
			else if (fieldName.compareToIgnoreCase("ofollowers_count") == 0) {
				if (val.compareToIgnoreCase("###") != 0) {
					ofollowers_count = Integer.parseInt(val);
				}
			}
			else if (fieldName.compareToIgnoreCase("text") == 0) {
//				if (val.compareToIgnoreCase("###") != 0) {
					text = val;
					try {
					if (val.contains(":")) {
						String[] parts = val.split(":");
						if (parts[0].contains("RT") && parts[0].contains("@")) {
							String[] parts2 = parts[0].split("\\s+");
							String[] temp = parts2[1].split("@");
							retweet_flag = true;
							retweetForUser = temp[1];
						}
					}
					} catch (Exception e) {
						retweet_flag = false;
						retweetForUser = "";
					}
//				}
			}
			else if (fieldName.compareToIgnoreCase("ufollowers_count") == 0) {
				if (val.compareToIgnoreCase("###") != 0) {
					ufollowers_count = Integer.parseInt(val);
				}
			}
			else if (fieldName.compareToIgnoreCase("oscreen_name") == 0) {
//				if (val.compareToIgnoreCase("###") != 0) {
					oscreen_name = val;
//				}
			}
			else if (fieldName.compareToIgnoreCase("in_reply_to_screen_name") == 0) {
				if (val.compareToIgnoreCase("None") != 0) {
					in_reply_to_screen_name = val;
				} else {
					in_reply_to_screen_name = "###";
				}
			}
			else if (fieldName.compareToIgnoreCase("uscreen_name") == 0) {
//				if (val.compareToIgnoreCase("###") != 0) {
					uscreen_name = val;
//				}
			}
			else if (fieldName.compareToIgnoreCase("ufavorite_count") == 0) {
				if (val.compareToIgnoreCase("###") != 0) {
					ufavorite_count = Integer.parseInt(val);
				}
			}
		}
	}


	@Override
	public void configure(JobConf config) {
		// TODO Auto-generated method stub
		try {
			ipHandles = new ArrayList<String>(Arrays.asList(config.get("handles").split("[,]+")));
			ipTags = new ArrayList<String>(Arrays.asList(config.get("tags").split("[,]+")));
			} catch (Exception e) {
				e.printStackTrace();
			}
	}
}

