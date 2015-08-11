from tweepy import Stream
from tweepy import OAuthHandler
from tweepy import API
import tweepy
from tweepy.streaming import StreamListener
import json,pprint,pickle,io,sys,time

ckey = 'wUI0mzhlWFDsodBjLVfANPOL3'
csecret = 'Y4bHDHf9Zh4rnPke9fIAnKlWDfqw9E2w5cAXu9gtvcK3KA822N'
atoken = '2921362476-PwwnNGpKST9KLIYCe1tTPNIqYyHoK3WCrhx3klf'
asecret = 'ur4pmU8m7dnMx2uwdCqSGnTJ2U5vUyN7a4yuKFF948fKT'

obj = ''
auth = OAuthHandler(ckey,csecret)
auth.set_access_token(atoken,asecret)
api = tweepy.API(auth) 
#search_tweets = api.search(screen_name='@Ferrari')
#timeline = user.timeline()
query = '@BMW OR #BMW OR @RollsRoyce OR #RollsRoyce OR #Porsche OR #Porsche \
		OR @Mercedes OR #Mercedes OR @Jaguar OR #Jaguar OR @Maserati OR \
		#Maserati OR @Bentley OR #Bentley OR @Audi OR #Audi OR @Lexus OR \
		#Lexus'

'''
query = @Ferrari OR #Ferrari OR #F1 OR @F1 OR @Formula1_GP OR #Formula1_GP \
		OR @Formula1 OR #AbuDhabiGP OR #FerrariSergio OR @McLarenF1 \
		OR #McLarenF1 OR @astonmartin OR @astonmartin OR @lamborghini \
		OR #lamborghini OR @VeyronBugatti OR #VeyronBugatti
'''

ferrari_dic = open('ferrari_dic.txt','a')
ferrari_txt = open('cars1.txt','wb')
dic = {}
cnt = 0
rep_tweet = []
for tweet in tweepy.Cursor(api.search, q = query,rpp=100).items():
	#data = json.loads(tweet)
	#pprint.pprint(data)
	#print tweet
	obj = tweet
	dic = {}
	user_mentions = []
	hashtags = []
	try:
		dic["uscreen_name"] = tweet.user.screen_name
		dic["ufollowers_count"] = tweet.user.followers_count
		dic["text"] = tweet.text
		dic["ufavorite_count"] = tweet.favorite_count
		dic["uretweet_count"] = tweet.retweet_count
		dic["in_reply_to_screen_name"] = tweet.in_reply_to_screen_name
	except tweepy.TweepError:
		time.sleep(60*15)
		continue
	except:
		pass
	try:
		#print 'user_mentions -------------'
		for i in tweet.entities["user_mentions"]:
			#print i["screen_name"]
			if i["screen_name"] not in user_mentions:
				user_mentions.append(i["screen_name"])
	except:
		pass
	try:
		for i in tweet.retweeted_status.entities["user_mentions"]:
			#print "Retweet user_mentions =   "+i["screen_name"]
			if i["screen_name"] not in user_mentions:
				user_mentions.append(i["screen_name"])
	except:
		pass
	dic["user_mentions"] = user_mentions
	try:
		#print 'Hashtags -------------'
		for i in tweet.entities["hashtags"]:
			#print i["text"]
			if i["text"] not in hashtags:
				hashtags.append(i["text"])
	except:
		pass
	try:
		for i in tweet.retweeted_status.entities["hashtags"]:
			if i["text"] not in hashtags:
				hashtags.append(i["text"])
	except:
		pass
	
	dic["hashtags"] = hashtags
	try:
		dic["oscreen_name"] = tweet.retweeted_status.user.screen_name
		dic["ofollowers_count"] = tweet.retweeted_status.user.followers_count
		dic["ofavorite_count"] = tweet.retweeted_status.favorite_count
		dic["oretweet_count"] = tweet.retweeted_status.retweet_count
	except:
		pass
	a = pickle.dumps(dic)
	#ferrari_dic.write(a)
	#pickle.dumps(dic, ferrari_dic)
	#encoding = sys.stdout.encoding or 'utf-8'
	text = ''
	for k in dic.keys():
		#print "Key="+str(k)+", Value="
		#if type(dic[k]) != int:
			#print str(dic[k].encode('utf-8')) 
		#else:
			#print dic[k]
		if k != "user_mentions" and k != "hashtags":
			text += str(k.encode('utf-8'))+"="
			if type(dic[k]) != int:
				#print "Key="+k + " Type = "+str(type(dic[k]))
				if dic[k] is None:
					text += str(dic[k])
				else:
					text += str(dic[k].encode('utf-8'))
				text = text.replace('\n',' ')
			else:
				text += str(dic[k])
		else:
			lst = dic[k]
			text += str(k.encode('utf-8'))+"="
			for i in lst:
				text += str(i.encode('utf-8'))+","
		text += " ||| "
		if dic["in_reply_to_screen_name"] is not None:
			#print "User - "+dic["uscreen_name"]+" , in Reply to = "+dic["in_reply_to_screen_name"]+"---------------\n"
			#print tweet
			reply = dic
			#rep_tweet.append(tweet)
	text += "\n\n"
	ferrari_txt.write(text)
#	print pickle.dumps(dic)
	#print "\nText = "+text
	cnt += 1
	print cnt
	if cnt>7000:
		break

ferrari_dic.close()
ferrari_txt.close()

#for item in timeline:
	##item = json.loads(item)
	##pprint.pprint(item)
	#print item
