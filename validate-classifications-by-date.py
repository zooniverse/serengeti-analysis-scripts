#!/usr/bin/env python
__author__ = 'alex'
import pymongo
import pickle

print "Scanning classifications db..."
# connect to the mongo server
client = pymongo.MongoClient()
db = client['serengeti3']
classification_collection = db["serengeti_classifications"]

# scan through *ALL* classifications and check which seasons present
for ii, classification in enumerate(classification_collection.find(no_cursor_timeout=True).skip(10000000)):  # .skip(skip).limit(limit)
  # skip tutorial classifications
  if "tutorial" in classification:
    continue
  if ii % 10000 == 0:
    print "%s: created at %s" % (ii,classification["created_at"])

print "\nDone.\n"

