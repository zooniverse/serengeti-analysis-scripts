#!/usr/bin/env python
__author__ = 'alex'
import pymongo
import csv
import pickle
from bson.objectid import ObjectId
from collections import OrderedDict

def create_csv(csv_filename):
  wrfile = open("summaries.csv", 'w')
  writer = csv.writer(wrfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
  writer.writerow(["season","group_id","new","returning","anon"])
  return {"handle": wrfile, "writer": writer}

# load pickle file
print "Loading pickle files...\n"
known_users = pickle.load( open( "known_users.p", "rb" ) )
anon_users_counts = pickle.load (open( "anon_users_counts.p", "rb") )

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

def get_season_no(oid):
  season = -1
  for i_season, i_oid in seasons.iteritems():
    if oid == i_oid:
      season = i_season
  if season==10:
    season=9
  elif season==9:
    season="Lost Season"
  return season

if seasons[1] in known_users.keys() and "alexbfree" in known_users[seasons[1]]:
  print "Alex is in season 1"
elif seasons[2] in known_users.keys() and "alexbfree" in known_users[seasons[2]]:
  print "Alex is in season 2"
elif seasons[3] in known_users.keys() and "alexbfree" in known_users[seasons[3]]:
  print "Alex is in season 3"
elif seasons[4] in known_users.keys() and "alexbfree" in known_users[seasons[4]]:
  print "Alex is in season 4"
elif seasons[5] in known_users.keys() and "alexbfree" in known_users[seasons[5]]:
  print "Alex is in season 5"
elif seasons[6] in known_users.keys() and "alexbfree" in known_users[seasons[6]]:
  print "Alex is in season 6"
elif seasons[7] in known_users.keys() and "alexbfree" in known_users[seasons[7]]:
  print "Alex is in season 7"
elif seasons[8] in known_users.keys() and "alexbfree" in known_users[seasons[8]]:
  print "Alex is in season 8"
elif seasons[9] in known_users.keys() and "alexbfree" in known_users[seasons[9]]:
  print "Alex is in the Lost Season"
elif seasons[10] in known_users.keys() and "alexbfree" in known_users[seasons[10]]:
  print "Alex is in season 9"
else:
  print "Alex's classifications were not encountered"

for k,v in known_users.iteritems():
  season = get_season_no(k)
  users = len(v)
  print "Season %s has %s known users" % (season, users)

for k,v in anon_users_counts.iteritems():
  season = get_season_no(k)
  anon_count = v
  print "Season %s has %s anonymous users" % (season, anon_count)