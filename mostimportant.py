import requests
import string
import logging
import time
import urlparse
import ConfigParser

from pymongo import MongoClient, errors as pymongoerrors


from pyechonest import config
from pyechonest import artist as echonest_artist
from pyechonest.util import EchoNestAPIError

import pylast


from goose import Goose

config = ConfigParser.ConfigParser()
config.read('influence.conf')

pyechonest.config.ECHO_NEST_API_KEY = config.get('main','ECHO_NEST_API_KEY') 

client = MongoClient(config.get('main', 'MONGO_CONN_STRING'))

auth = client.influence.authenticate(config.get('main', 'MONGO_USER'), config.get('main', 'MONGO_PASSWD'))

db = client.influence

found_count = 0

mg_api_time = 0

def clear_db():

	db.artist.remove()

def setup_logging():

	logging.basicConfig(filename='log.log', level=logging.DEBUG)

def find_insert(find_fields, insert_fields, musicgraph_update=True, echonest_update=True):

	global found_count

	found_artist = db.artist.find_one(find_fields)
	
	if found_artist == None:
		found_count = 0
		print "Didn't find it"
		print "Inserting"
		print dict(insert_fields, **{"update_status":"Active"})
		artist_id = db.artist.insert(dict(insert_fields, **{"update_status":{"musicgraph":"No", "echonest":"No"}}))
		
		found_artist = db.artist.find_one({"_id":artist_id})

		if 'musicgraph_id' not in found_artist:
			db.artist.update({"_id":artist_id}, {"$set":{"musicgraph_id":get_mg_id(found_artist)}})

		try:
			echonest_id = found_artist['echonest_data']['echonest_id']
		except KeyError:
			db.artist.update({"_id":artist_id}, {"$set":{"echonest_data":{"echonest_id":get_en_id(found_artist)}}})

		found_artist = db.artist.find_one({"_id":artist_id})

		if musicgraph_update==True:
			update_mg(found_artist)
			db.artist.update({"_id":artist_id}, {"$set":{"update_status.musicgraph":"Yes"}})
		if echonest_update == True:
			append_en(found_artist)
			db.artist.update({"_id":artist_id}, {"$set":{"update_status.echonest":"Yes"}})

	else:
		found_count +=1 
		print "Found it - {0}".format(found_count)
		artist_id = found_artist['_id']

	if musicgraph_update == True:
		update_mg(found_artist)
		db.artist.update({"_id":artist_id}, {"$set":{"update_status.musicgraph":"Yes"}})

	if echonest_update == True:
		append_en(found_artist)
		db.artist.update({"_id":artist_id}, {"$set":{"update_status.echonest":"Yes"}})

	return found_artist

def get_mg_api(params):

	try:

		#OLD API KEY 
		#api_param = {'api_key': '62c183d2b4a900aac93f2a17f134bba7', 'limit': 100}
		api_param = {'api_key': config.get('main', 'MUSICGRAPH_API_KEY'), 'limit': 100}

		if 'id' in params:
			url = 'http://api.musicgraph.com/api/v2/artist/'+params['id']
			request = requests.get(url, params=api_param)
			
		else:
			url = 'http://api.musicgraph.com/api/v2/artist/search'
			request = requests.get(url, params=dict(api_param, **params))


		if request.json()["status"]["code"] != 0:
			raise Exception("API message {0}".format(request.json()["status"]["message"]))
		else:
			return request

	except requests.ConnectionError as e:
		print e
		print "MG sleeping 10"
		time.sleep(10)
		get_mg_api(params)


def get_mg_id(artist):

	mg_artist = get_mg_api({'name':artist['name']}).json()['data']
	
	if len(mg_artist) > 0:
		return mg_artist[0]['id']
	else:
		return ''

def update_mg(artist):

	print 'MusicGraph: {0}'.format(artist['name'].encode('ascii', 'ignore'))

	if 'musicgraph_id' in artist:
		if len(artist['musicgraph_id'])>0:
			mg_artist = get_mg_api({'id':artist['musicgraph_id']}).json()['data']
		else:
			mg_artist = {}
		
	else:	
		mg_artist = get_mg_api({'name':artist['name']}).json()['data']
		if len(mg_artist) > 0:
			mg_artist = mg_artist[0]
	
	if len(mg_artist) > 0:
		db.artist.update({"_id":artist['_id']}, {"$set":{"musicgraph_id":mg_artist['id'], "amg_pop_id":mg_artist['amg_pop_id'] if 'amg_pop_id' in mg_artist else ''}})
		print 'get mg influencers'
		get_mg_influencers(artist)
		print 'get mg similar'
		get_mg_similar(artist)

def get_artist_by_id(id):

	return db.artist.find_one({"_id": id})

def get_en_id(artist):

	try:

		en_artist = echonest_artist.Artist(artist['name'])

		if hasattr(en_artist, 'id'):
			return en_artist.id
		else:
			return ''

	except EchoNestAPIError:

		return ''

def append_en(artist):

	try:
		if "echonest_id" in artist['echonest_data']:
			en_artist = echonest_artist.Artist(artist['echonest_data']['echonest_id'])
		else:
			en_artist = echonest_artist.Artist(artist['name'])
		
		print 'Echonest: {0}'.format(en_artist)
		if en_artist.familiarity != None:
			
			db.artist.update({"_id": artist['_id']},

				{"$set":{

				"echonest_data": 
					{"echonest_id": en_artist.id,
					"familiarity": en_artist.familiarity,
					"years_active":en_artist.years_active}

					}})

			extract_review_artists(dict(artist, **{"echonest_data":{"echonest_id": en_artist.id}}))
			print 'get en similar'
			get_en_similar(dict(artist, **{"echonest_data":{"echonest_id": en_artist.id}}))

	except EchoNestAPIError as e:
		print e	
		if e.code == 3:
			print "sleeping 10 seconds"
			time.sleep(10)
			append_en(artist)

def extract_review_artists(artist):

	print ('Extracting reviews: {0}').format(artist['name'].encode('ascii', 'ignore'))
	try:
		en_artist = echonest_artist.Artist(artist['echonest_data']['echonest_id'])

		print en_artist.reviews
	
		for review in en_artist.reviews:
			url = review['url']
			domain = urlparse.urlparse(url).netloc
			
			try:
				text = Goose().extract(url).cleaned_text
				review_artists = []

				for extracted_artist in echonest_artist.extract(text, results=99):
					print extracted_artist
					review_artists.append(find_insert({"name":extracted_artist.name}, {"name":extracted_artist.name,"echonest_data":{"echonest_id":extracted_artist.id}}, False, False)['_id'])
			
				db.artist.update({"_id": artist['_id']}, {"$set": {"echonest_data.review_artists":review_artists}})	

			except Exception as e:
				print 'Goose Error {0}'.format(e)
			
	except EchoNestAPIError as e:
		print e	
		if e.code == 3:
			print "sleeping 10 seconds"
			time.sleep(10)
			extract_review_artists(artist)
	
	
def append_mg(id):

	artist = get_artist_by_id(id)
	mg_artist = get_mg_api({'id': artist['musicgraph_id']})

def similar_func():
	params = {'influenced': 'Radiohead'} 

	r = get_mg_api(params)

	for element in r.json()['data']:

		print str(element['name'])+'\n\n'

def upsert_artist(name, musicgraph_id, amg_pop_id):

	return db.artist.update({"name" : name, "musicgraph_id" : musicgraph_id, "amg_pop_id" : amg_pop_id}, {"$set" :  {"name": name, "musicgraph_id" : musicgraph_id, "amg_pop_id" : amg_pop_id}}, upsert=True)

def get_mg_influencers(artist):

	for y in range(1, 10000, 100):

		influencers = get_mg_api({'influenced': artist['name'], 'offset': y})

		if len(influencers.json()['data']) == 0:
			break
		else:
			for influencer in influencers.json()['data']:
				
				print 'Influencer: {0} {1}'.format(influencer['name'].encode('ascii', 'ignore'), influencer['id'])

				influencer_insert = find_insert({"musicgraph_id":influencer['id']}, {"musicgraph_id":influencer['id'], "name":influencer['name'], "amg_pop_id":influencer['amg_pop_id'] if 'amg_pop_id' in influencer else ''}, False, False)
				
				if hasattr(influencer_insert, 'influenced') == False:

					influencee_ids = []

					for x in range(1, 10000, 100):

						influenced = get_mg_api({'influenced_by': influencer['name'], 'offset': x})

						if len(influenced.json()['data']) == 0:
							break
						else:							

							for influencee in influenced.json()['data']:

								print 'Influencee: {0} {1}'.format(influencee['name'].encode('ascii', 'ignore'), influencee['id'])

								influencee_insert = find_insert({"musicgraph_id":influencee['id']}, {"musicgraph_id":influencee['id'],"name":influencee['name'], "amg_pop_id":influencee['amg_pop_id'] if 'amg_pop_id' in influencee else ''}, False, False)
								influencee_ids.append(influencee_insert['_id'])
							
					update = db.artist.update({"_id" : influencer_insert['_id']}, {"$addToSet": {"influenced": {"$each": influencee_ids}}})

def get_mg_similar(artist):

	similar_list = []

	for y in range(1, 10000, 100):

		similars = get_mg_api({'similar_to': artist['name'], 'offset': y})

		if len(similars.json()['data']) == 0:
			break
		else:
			for similar in similars.json()['data']:
				
				print 'Musicgraph similar: {0}'.format(similar['name'].encode('ascii', 'ignore'), similar['id'])

				similar_insert = find_insert({"musicgraph_id":similar['id']}, {"musicgraph_id":similar['id'],"name":similar['name'], "amg_pop_id":similar['amg_pop_id'] if 'amg_pop_id' in similar else ''}, False, False)		
				similar_list.append(similar_insert['_id'])
		
	update = db.artist.update({"_id" : artist['_id']}, {"$set": {"musicgraph_similar": similar_list}})

def get_en_similar(artist):
	similar_list = []

	similars = echonest_artist.similar(artist['echonest_data']['echonest_id'], results=99)

	if len(similars) > 0:
		for similar in similars:

			print "Echonest similar: {0}".format(similar)
			
			similar_insert = find_insert({"echonest_data.echonest_id":similar.id}, {"name":similar.name, "echonest_data":{"echonest_id":similar.id}}, False, False)
			similar_list.append(similar_insert['_id'])

		update = db.artist.update({"_id" : artist['_id']}, {"$set": {"echonest_data.echonest_similar": similar_list}})

def update_musicgraph_artists():

	try:
		artists = db.artist.find({"update_status.musicgraph":"No"})
		for artist in artists:
			print artist['_id']
			if 'musicgraph_id' in artist:
				if artist['musicgraph_id'] != '':
					print "FOUND ID"
					find_insert({"musicgraph_id": artist['musicgraph_id']},{}, True, True)
				else:
					print "Musicgraph ID blank"
					db.artist.update({"name":artist['name']}, {"$set": {"update_status.musicgraph":"Yes"}})
			else:
				find_insert({"name":artist['name']}, {}, True, True)
	except pymongoerrors.CursorNotFound as e:
		print e
		main()

def check_last_fm_top_artists():

	network = pylast.LastFMNetwork(api_key='834c4d1bec6311297f3c29e8f0c91387', api_secret='c8326f260180dd3f99d4477892cecbb3')
	top_artists = network.get_top_artists()
	for top_artist in top_artists:
		print '***LAST FM ARTIST {0}\n\n'.format(top_artist.item.name)
		find_insert({"name":top_artist.item.name}, {}, True, True)

if __name__ == '__main__':

	try:
		check_last_fm_top_artists()
	except pymongoerrors.CursorNotFound as e:
		print e
		main

	#clear_db()
	#find_insert({"name":"Hank Williams"}, {"name":"Hank Williams"}, True, True)

	
	#print echonest_artist.similar("AR51USL1187FB44216", results=100)

	#print upsert_artist('Louis Armstrong')
	#main()

    ##################
	#artists = db.artist.find({"echonest_data":{"$exists": False}})
	
	#for artist in artists:
	#	print artist['name'].encode("ascii", 'ignore')
	#	append_echonest(artist["_id"])
	###################

	#########################
	#artists = db.artist.find()
	
	#for artist in artists:
	#	print artist['_id']
	#	extract_review_artists(artist["_id"])
	#########################


	#print requests.get(en_artist.reviews[0]['url']).content
	#print en_artist.reviews[0]

	#append_echonest(db.artist.find_one({"name":"Uncle Tupelo"})["_id"])

	#print get_mg_api({"id" : "931539bf-8e62-c642-a116-9ee3e0758e43"}).json()['data']['discogs_url'][0].split('/')[4]