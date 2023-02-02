import logging
import sys

from extract_hashtag import hashtag_service
from sentiments_analysis import sentiment_service
from id_topic import topic_service
from collections import abc

from spyne.protocol.soap import Soap11
from spyne.server.wsgi import WsgiApplication
from spyne.util.wsgi_wrapper import run_twisted
from spyne import Application


application_hashtag = Application([hashtag_service],
    tns='hashtag.service',
    in_protocol=Soap11(validator='lxml'),
    out_protocol=Soap11()
)
application_sentiment = Application([sentiment_service],
    tns='sentiment.service',
    in_protocol=Soap11(validator='lxml'),
    out_protocol=Soap11()
)
application_topic = Application([topic_service],
    tns='topic.service',
    in_protocol=Soap11(validator='lxml'),
    out_protocol=Soap11()
)

if __name__ == '__main__':

############# DEBUG INFOS ###################################
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('spyne.protocol.xml').setLevel(logging.DEBUG)

    logging.info("listening to http://127.0.0.1:8000/%22")
    logging.info("wsdl is at: http://localhost:8000/?wsdl%22")
##############################################################

    wsgi_app_hashtag = WsgiApplication(application_hashtag)
    wsgi_app_sentiment = WsgiApplication(application_sentiment) 
    wsgi_app_topic = WsgiApplication(application_topic) 

    twisted_apps = [
        (wsgi_app_hashtag, b'hashtag_service'), 
        (wsgi_app_sentiment, b'sentiment_service'),
        (wsgi_app_topic, b'topic_service')
        ]
        
    sys.exit(run_twisted(twisted_apps, 8000))

    