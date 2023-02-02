from textblob import TextBlob
import statistics
import logging
logging.basicConfig(level=logging.DEBUG)

from spyne import rpc, ServiceBase, \
    Unicode, Float, AnyDict, AnyUri

class sentiment_service(ServiceBase):
    
    @rpc(Unicode, _returns=Float)
    def sentiment_analysis(ctx, tweet):
        blob = TextBlob(tweet)
        return statistics.mean([s.sentiment.polarity for s in blob.sentences])

