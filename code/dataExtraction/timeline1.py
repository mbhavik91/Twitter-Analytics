from tweepy import Stream
from tweepy import OAuthHandler
from tweepy import API
import tweepy
from tweepy.streaming import StreamListener
import json,pprint

ckey = 'wUI0mzhlWFDsodBjLVfANPOL3'
csecret = 'Y4bHDHf9Zh4rnPke9fIAnKlWDfqw9E2w5cAXu9gtvcK3KA822N'
atoken = '2921362476-PwwnNGpKST9KLIYCe1tTPNIqYyHoK3WCrhx3klf'
asecret = 'ur4pmU8m7dnMx2uwdCqSGnTJ2U5vUyN7a4yuKFF948fKT'

obj = ''

class listener(StreamListener):
	
	def on_data(self,data):
		global obj
		obj = data
		#print type(data["text"])
		data = json.loads(data)
		pprint.pprint(data)
		return True
	
	def on_error(self,status):
		print status


auth = OAuthHandler(ckey,csecret)
auth.set_access_token(atoken,asecret)
api = tweepy.API(auth) 
user = api.get_user(screen_name='Ferrari')
timeline = user.timeline()

i = 0
for status in tweepy.Cursor(api.user_timeline, id = "Ferrari").items():
	#data = json.loads(status)
	pprint.pprint(status)
	i+=1
	if i>1:
		break

#for item in timeline:
	##item = json.loads(item)
	##pprint.pprint(item)
	#print item
