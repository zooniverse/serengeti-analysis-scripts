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

pageSize = 5000

page_one = enumerate(classification_collection.find().limit(pageSize))
completed_page_rows = 0
for ii, classification in page_one:
  completed_page_rows+=1
  last_id = classification["_id"]

next_results = classification_collection.find({"_id":{"$gt":last_id}},no_cursor_timeout=True).limit(pageSize)
while next_results.count()>0:
  for ii, classification in enumerate(next_results):
    completed_page_rows+=1
    if completed_page_rows % 10000 == 0 or completed_page_rows > 10593400:
      print "%s (id = %s): created at %s" % (completed_page_rows,classification["_id"],classification["created_at"])
    last_id = classification["_id"]
  next_results = classification_collection.find({"_id":{"$gt":last_id}}).limit(pageSize)

print "\nDone.\n"

