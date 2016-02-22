#!/usr/bin/env python
__author__ = 'alex'
import pymongo
import pickle

# connect to the mongo server
client = pymongo.MongoClient()
db = client['serengeti3']
classification_collection = db["serengeti_classifications"]
subject_collection = db["serengeti_subjects"]
user_collection = db["serengeti_users"]

# for storing the users seen for each season
subject_season_map = {}

# scan through subjects to track which belongs to which season
for ii, subject in enumerate(subject_collection.find(no_cursor_timeout=True)):  # .skip(skip).limit(limit)
  if "group_id" in subject.keys():
    if ii % 10000 == 0:
      print ii
    subject_id = subject["zooniverse_id"]
    group_id = subject["group_id"]
    subject_season_map[subject_id]= group_id
  else:
    print "no season for subject "+subject["zooniverse_id"]

pickle.dump(subject_season_map, open( "subject_season_map.p", "wb" ) )