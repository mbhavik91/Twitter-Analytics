from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json,pprint

ckey = 'wUI0mzhlWFDsodBjLVfANPOL2'
csecret = 'Y4bHDHf9Zh4rnPke9fIAnKWDfqw9E2w5cAXu9gtvcK3KA822N'
atoken = '2921362476-PwwnNGpKST9LIYCe1tTPNIqYyHoK3WCrhx3klf'
asecret = 'ur4pmU8m7dnMx2uwdCqSnTJ2U5vUyN7a4yuKFF948fKT'

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
twitterStream = Stream(auth,listener())
twitterStream.filter(track=["AusVsInd","IndVsAus"])
