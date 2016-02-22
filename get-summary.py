#!/usr/bin/env python
__author__ = 'alex'
import pymongo
import csv
import pickle
from bson.objectid import ObjectId
from collections import OrderedDict
import sys

def create_csv(csv_filename):
  wrfile = open("summaries.csv", 'w')
  writer = csv.writer(wrfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
  writer.writerow(["season","group_id","new","returning","anon"])
  return {"handle": wrfile, "writer": writer}

# load pickle file
print "Loading pickle files...\n"
known_users = pickle.load( open( "known_users.p", "rb" ) )
anon_users_counts = pickle.load (open( "anon_users_counts.p", "rb") )

def get_season_str(season):
  if season==9:
    season_str="Lost Season"
  elif season==10:
    season_str=9
  else:
    season_str=str(season)
  return season_str

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

users_seen_before = []
summary_per_season = {}

print "Analysing season summaries..."
for season, group_id in seasons.iteritems():
  season_str=get_season_str(season)
  print "Analysing season %s..." % season_str
  if season not in summary_per_season.keys():
    summary_per_season[season]={"new":0,"returning":0,"anon":0}
  if group_id in known_users:
    for user in known_users[group_id]:
      if user in users_seen_before:
        summary_per_season[season]["returning"]+=1
      else:
        summary_per_season[season]["new"]+=1
        users_seen_before.append(user)
  else:
    summary_per_season[season]["new"]="N/A"
    summary_per_season[season]["returning"]="N/A"
  if group_id in anon_users_counts:
    summary_per_season[season]["anon"] = anon_users_counts[group_id]
  else:
    summary_per_season[season]["anon"] = "N/A"

print "\nWriting CSV...\n"
csvwriter = create_csv("summaries.csv")
for season, summary in summary_per_season.iteritems():
  season_str=get_season_str(season)
  csvwriter["writer"].writerow([season_str,seasons[season],summary["new"],summary["returning"],summary["anon"]])
  print "Season %s has %s new users, %s returning users, and %s anonymous users." % (season_str,summary["new"],summary["returning"],summary["anon"])
csvwriter["handle"].close()
print "\nDone.\n"