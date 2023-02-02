import collections
import logging
import re
import random
from pymongo import MongoClient
logging.basicConfig(level=logging.DEBUG)

from spyne import rpc, ServiceBase, \
    Unicode, Integer, Array, AnyDict

class topic_service(ServiceBase):
    @rpc(AnyDict, _returns=Unicode)
    def topic_identification(ctx, tweet):
        topics = ["sport", "politique", "sciences"]
        return random.choice(topics)


    @rpc(Array(Unicode), Integer, _returns=Array(Unicode))
    def top_topics(ctx, topics, K):
        return collections.Counter(topics).most_common(K)

