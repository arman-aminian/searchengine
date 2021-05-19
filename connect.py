import logging
import sys

from elasticsearch import Elasticsearch

from searchengine.config import connection_config

logging.basicConfig(level=logging.INFO)


es = Elasticsearch([connection_config])

if not es.ping():
    logging.error('connection to elastic search failed:\nconfig:{}'.format(connection_config))
    sys.exit()
