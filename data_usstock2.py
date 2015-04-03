__author__ = 'summit'

import json
import time
import pandas as pd
from elasticsearch import Elasticsearch

INDEX_NAME = 'usstock2'

ES_HOST = {"host" : "104.236.193.82", "port" : 9200}
es = Elasticsearch(hosts = [ES_HOST])

# res = es.search(index = INDEX_NAME, size=2, body={"query": {"match_all": {}}})
returnFields = ['symbol','high','low','open','close','volume','timestamp']
returnQuery = {"query": {"match_all": {}}}
res = es.search(index = INDEX_NAME, fields = [], body = returnQuery)
returnSize = res['hits']['total']
res = es.search(index = INDEX_NAME, size = returnSize, fields = returnFields, body = returnQuery)
# print(" response: '%s'" % (res))

Data = res['hits']['hits']
DF = pd.concat(map(pd.DataFrame.from_dict, Data), axis=1)['fields'].T

print DF.reset_index(drop=True)
