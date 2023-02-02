import logging
import collections

from pymongo import MongoClient
logging.basicConfig(level=logging.DEBUG)
import re

from spyne import rpc, ServiceBase, \
    Unicode, Integer, Array, AnyDict, AnyUri

class hashtag_service(ServiceBase):
    
    @rpc(Unicode, _returns=Array(Unicode))
    def extract_hashtag(ctx, tweet):
        if "#" in tweet:
            return re.findall(r"#(\w+)", tweet)
        else:
            return ["any"]


    @rpc(Array(Unicode), Integer, _returns=Array(Unicode))
    def top_hashtags(ctx, hashtags, K): 
        return collections.Counter(hashtags).most_common(K)


