#!/usr/bin/env python
__author__ = 'alex'
import pymongo
import pickle

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
  season = subject_season_map[subject_id]
  if not season_found[season]:
    season_found[season]=True

print "\nClassification Season Check Results:\n"
for season,status in season_found.iteritems():
  if status:
    print "For Season %s, classifications were found."
  else:
    print "For Season %s, NO CLASSIFICATIONS FOUND."

print "\nDone.\n"

