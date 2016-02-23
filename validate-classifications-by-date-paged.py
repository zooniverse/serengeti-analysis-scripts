#!/usr/bin/env python
__author__ = 'alex'
import pymongo
import pickle

print "Scanning classifications db..."
# connect to the mongo server
client = pymongo.MongoClient()
db = client['serengeti3']
classification_collection = db["serengeti_classifications"]

print "Collection contains %s documents." % db.command("collstats", "serengeti_classifications")["count"]

pageSize = 100000
first_classification = classification_collection.find_one()
completed_page_rows=1
last_id = first_classification["_id"]

next_results = classification_collection.find({"_id":{"$gt":last_id}},{"tutorial":1,"created_at":1},no_cursor_timeout=True).limit(pageSize)
while next_results.count()>0:
  for ii, classification in enumerate(next_results):
    completed_page_rows+=1
    if completed_page_rows % pageSize == 0:
      print "%s (id = %s): created at %s" % (completed_page_rows,classification["_id"],classification["created_at"])
    last_id = classification["_id"]
  next_results = classification_collection.find({"_id":{"$gt":last_id}},{"tutorial":1,"created_at":1},no_cursor_timeout=True).limit(pageSize)

print "\nDone.\n"

