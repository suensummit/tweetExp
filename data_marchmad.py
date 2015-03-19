__author__ = 'summit'

import json
import urllib
import time
import dateutil.parser
from elasticsearch import Elasticsearch

# api
response = json.load(urllib.urlopen("http://marchmadness.kimonolabs.com/api/players?apikey=Pbkz9h0O3z7LnZ8QD8njaGzRsS63ZaEf"))
# response = json.load(urllib.urlopen("http://marchmadness.kimonolabs.com/api/teams?apikey=Pbkz9h0O3z7LnZ8QD8njaGzRsS63ZaEf"))
# response = json.load(urllib.urlopen("http://marchmadness.kimonolabs.com/api/conferences?apikey=Pbkz9h0O3z7LnZ8QD8njaGzRsS63ZaEf"))
# response = json.load(urllib.urlopen("http://marchmadness.kimonolabs.com/api/games?apikey=Pbkz9h0O3z7LnZ8QD8njaGzRsS63ZaEf"))
# response = json.load(urllib.urlopen("http://marchmadness.kimonolabs.com/api/team_game_stats?apikey=Pbkz9h0O3z7LnZ8QD8njaGzRsS63ZaEf"))
# response = json.load(urllib.urlopen("http://marchmadness.kimonolabs.com/api/player_game_stats?apikey=Pbkz9h0O3z7LnZ8QD8njaGzRsS63ZaEf"))
# set host
ES_HOST = {"host" : "104.236.193.82", "port" : 9200}
# set index
INDEX_NAME = 'march-mad'
TYPE_NAME = 'Player'
# TYPE_NAME = 'Team'
# TYPE_NAME = 'Conference'
# TYPE_NAME = 'Game'
# TYPE_NAME = 'TeamGameStat'
# TYPE_NAME = 'PlayerGameStat'
ID_FIELD = 'id'
# create ES client, create index
es = Elasticsearch(hosts = [ES_HOST])
# set bulk data
bulk_data = []
for row in response:
    data_dict = {}
    data_dict = row
    op_dict = {
        "index": {
        	"_index": INDEX_NAME,
        	"_type": TYPE_NAME,
        	"_id": data_dict[ID_FIELD]
        }
    }
    bulk_data.append(op_dict)
    bulk_data.append(data_dict)
# # clear index
# if es.indices.exists(INDEX_NAME):
#     print("deleting '%s' index..." % (INDEX_NAME))
#     res = es.indices.delete(index = INDEX_NAME)
#     print(" response: '%s'" % (res))
# # create index and insert bulk data
# res = es.indices.create(index = INDEX_NAME)
res = es.bulk(index = INDEX_NAME, body = bulk_data, refresh = True)
# verify
res = es.search(index = INDEX_NAME, size=2, body={"query": {"match_all": {}}})
print(" response: '%s'" % (res))

# res = es.index(
#     index="march-mad", doc_type=ts, id=ts, body={
#         'timestamp':ts,
#         'text':tweet["text"]
#     })
