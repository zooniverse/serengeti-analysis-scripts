#!/usr/bin/env python
__author__ = 'alex'
import pymongo
from collections import OrderedDict
from bson import ObjectId
from datetime import datetime,timedelta
import unicodecsv as csv
import sys
import os
import time

DATABASE_NAME="talk"
PROJECT_NAME="serengeti"

def restart_line():
  sys.stdout.write('\r')
  sys.stdout.flush()

print "\nScanning discussions db and generating post summaries..."
# connect to the mongo server
client = pymongo.MongoClient()
db = client[DATABASE_NAME]
projects_collection = db["projects"]
discussions_collection = db["discussions"]

# get project id
project = projects_collection.find_one({"name":PROJECT_NAME})
project_id = project["_id"]

# scan through *ALL* discussions (add a .skip or .limit to look at subset)
pageSize = 10000
completed_page_rows=1

wrfile = open("csvs/output/daily-summary/talk-user-summaries.csv", 'w')
writer = csv.writer(wrfile, delimiter=',', quoting=csv.QUOTE_NONNUMERIC,dialect='excel', encoding='utf-8')
writer.writerow(["Thread ID","Comment ID","User ID","Date and Time of Comment","Post Type", "Is A Reply"])

for ii, discussion in enumerate(discussions_collection.find({"project_id":project_id}, {'focus.base_type':1,'comments.user_name':1,'comments.created_at':1}, no_cursor_timeout=True)):
  completed_page_rows+=1
  if completed_page_rows % 1000 == 0:
    restart_line()
    sys.stdout.write("%s discussions examined..." % completed_page_rows)
    sys.stdout.flush()
  comment_no = 0
  for comment in discussion["comments"]:
    row = [discussion["_id"], comment_no, comment["user_name"], comment["created_at"].strftime('%Y-%m-%d %H:%M:%S'), discussion['focus']['base_type'], comment_no>0]
    comment_no += 1
    outrow = []
    for el in row:
      if isinstance(el,str):
        outrow.append(unicode(el.decode('utf-8')))
      else:
        outrow.append(el)
    writer.writerow(outrow)

restart_line()
sys.stdout.write("%s discussions processed..." % completed_page_rows)
sys.stdout.flush()

print "\n\nProcessed a total of %s discussion threads." % (completed_page_rows)
print "\nExporting user post summaries to CSV..."

wrfile.close()

print "\nDone.\n"
