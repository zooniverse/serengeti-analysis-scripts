#!/usr/bin/env python
__author__ = 'alex'
import pickle
from bson.objectid import ObjectId

# load pickle file
print "Loading season mapping pickle file...\n"
subject_season_map = pickle.load( open( "subject_season_map.p", "rb" ) )

season_subject_map = {}

print "Re-indexing by season..."
i=0
for subject,season_oid in subject_season_map.iteritems():
  #print "subject is %s season is %s" % (subject, season)
  i += 1
  if i % 10000 == 0:
    print i
  if not season_oid in season_subject_map.keys():
    season_subject_map[season_oid]=0
  season_subject_map[season_oid] += 1

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

print "Finished re-indexing by season. Counts are:\n"
for season_oid,subject in season_subject_map.iteritems():
  print "Season %s has %s subjects" % (get_season_no(season_oid), season_subject_map[season_oid])