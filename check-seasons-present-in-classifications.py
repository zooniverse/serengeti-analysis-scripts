#!/usr/bin/env python
__author__ = 'alex'
import pymongo
import pickle
from bson.objectid import ObjectId

# load pickle file
print "Loading season mapping pickle file...\n"
subject_season_map = pickle.load( open( "subject_season_map.p", "rb" ) )

season_subject_map = {}

print "Re-indexing by season...\n"
i=0
for subject,season in subject_season_map.iteritems():
  i += 1
  if i % 10000 == 0:
    print i
  if not season in season_subject_map.keys():
    season_subject_map[season]=0
  season_subject_map[season] += 1

print "Finished re-indexing by season. Counts are:\n"
for season,subject in season_subject_map.iteritems():
  print "Season %s has %s subjects" % (season, season_subject_map[season])


print "Scanning classifications db..."
# connect to the mongo server
client = pymongo.MongoClient()
db = client['serengeti3']
classification_collection = db["serengeti_classifications"]

seasons = {
  1: ObjectId("50c6197ea2fc8e1110000001"),
  2: ObjectId("50c61e51a2fc8e1110000002"),
  3: ObjectId("50c62517a2fc8e1110000003"),
  4: ObjectId("50e477293ae740a45f000001"),
  5: ObjectId("51ad041f3ae7401ecc000001"),
  6: ObjectId("51f158983ae74082bb000001"),
  7: ObjectId("5331cce91bccd304b6000001"),
  8: ObjectId("54cfc76387ee0404d5000001"),
  0: ObjectId("55a3d6cf3ae74036bc000001"),
  9: ObjectId("56a63b3b41479b0042000001")
}

def get_season_no(oid):
  season = -1
  for i_season, i_oid in seasons.iteritems():
    if oid == i_oid:
      season = i_season
  return season

season_found = {
  1: False,
  2: False,
  3: False,
  4: False,
  5: False,
  6: False,
  7: False,
  8: False,
  0: False, # (lost season)
  9: False
}

# scan through *ALL* classifications and check which seasons present
for ii, classification in enumerate(classification_collection.find(no_cursor_timeout=True)):  # .skip(skip).limit(limit)
  # skip tutorial classifications
  if "tutorial" in classification:
    continue
  if ii % 10000 == 0:
    print ii
  subject_id = classification["subjects"][0]["zooniverse_id"]
  season_oid = subject_season_map[subject_id]
  season_num = get_season_no(season_oid)
  if not season_found[season_num]:
    season_found[season_num]=True

print "\nClassification Season Check Results:\n"
for season_num,status in season_found.iteritems():
  if status:
    print "For Season %s, classifications were found." % season_num
  else:
    print "For Season %s, NO CLASSIFICATIONS FOUND." % season_num

print "\nDone.\n"

