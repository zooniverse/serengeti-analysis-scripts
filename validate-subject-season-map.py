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
  print "subject is %s season is %s" % (subject, season)
  i += 1
  if i % 10000 == 0:
    print i
  if not season in season_subject_map.keys():
    season_subject_map[season]=0
  season_subject_map[season] += 1

print "Finished re-indexing by season. Counts are:\n"
for season,subject in season_subject_map.iteritems():
  print "Season %s has %s subjects" % (season, season_subject_map[season])