#!/usr/bin/env python
__author__ = 'alex'
import pymongo
import pickle
from bson.objectid import ObjectId
from collections import OrderedDict

seasons = {
  1: ObjectId("50c6197ea2fc8e1110000001"),
  2: ObjectId("50c61e51a2fc8e1110000002"),
  3: ObjectId("50c62517a2fc8e1110000003"),
  4: ObjectId("50e477293ae740a45f000001"),
  5: ObjectId("51ad041f3ae7401ecc000001"),
  6: ObjectId("51f158983ae74082bb000001"),
  7: ObjectId("5331cce91bccd304b6000001"),
  8: ObjectId("54cfc76387ee0404d5000001"),
  9: ObjectId("55a3d6cf3ae74036bc000001"), # actually Lost Season
  10: ObjectId("56a63b3b41479b0042000001") # actually Season 9
}

seasons = OrderedDict((season,oid) for season,oid in seasons.iteritems())

def get_season_str(oid):
  season = -1
  for i_season, i_oid in seasons.iteritems():
    if oid == i_oid:
      season = i_season
  if season==10:
    season=9
  elif season==9:
    season="Lost Season"
  return season

# load pickle file
print "Loading season mapping pickle file...\n"
subject_season_map = pickle.load( open( "pickle/subject_season_map.p", "rb" ) )

print "Scanning classifications db..."
# connect to the mongo server
client = pymongo.MongoClient()
db = client['serengeti3']
classification_collection = db["serengeti_classifications"]

# for storing the users seen for each season
known_users = {}
anon_users_counts = {}
user_first_encountered_in_season = {}

# scan through *ALL* classifications (add a .skip or .limit to look at subset)
pageSize = 500000
first_classification = classification_collection.find_one()
completed_page_rows=1
last_id = first_classification["_id"]
last_classification=None
skipped = 0
latest_date=first_classification["created_at"]

next_results = classification_collection.find({"_id":{"$gt":last_id}},{"created_at":1,"tutorial":1,"user_name":1,"subjects":1},no_cursor_timeout=True).limit(pageSize)
while next_results.count()>0:
  for ii, classification in enumerate(next_results):
    completed_page_rows+=1
    if completed_page_rows % pageSize == 0:
      print "%s (id = %s)" % (completed_page_rows,classification["_id"])
    last_id = classification["_id"]
    latest_date = classification["created_at"]
    if "tutorial" in classification.keys() and classification["tutorial"]==True:
      subject_id = classification["subjects"][0]["zooniverse_id"]
      season = subject_season_map[subject_id]
      skipped += 1
      continue
    else:
      subject_id = classification["subjects"][0]["zooniverse_id"]
      season = subject_season_map[subject_id]
      if "user_name" in classification.keys():
        user_name = classification["user_name"]
        if user_name not in user_first_encountered_in_season.keys():
          user_first_encountered_in_season[user_name]=season
        if season not in known_users.keys():
          known_users[season]=[]
        if season >= user_first_encountered_in_season[user_name] and user_name not in known_users[season]:
          known_users[season].append(user_name)
      else:
        if season not in anon_users_counts.keys():
          anon_users_counts[season]=0
        anon_users_counts[season] += 1
  next_results = classification_collection.find({"_id":{"$gt":last_id}},{"created_at":1,"tutorial":1,"user_name":1,"subjects":1},no_cursor_timeout=True).limit(pageSize)

print "Processed a total of %s documents (skipped %s). The latest date examined was %s" % (completed_page_rows,skipped,latest_date)

print "Saving known users and anonymous users..."

pickle.dump( known_users, open( "pickle/known_users.p", "wb" ) )
pickle.dump( anon_users_counts, open( "pickle/anon_users_counts.p", "wb" ) )