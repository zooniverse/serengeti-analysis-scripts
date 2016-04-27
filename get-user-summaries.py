#!/usr/bin/env python
__author__ = 'alex'
import pymongo
from collections import OrderedDict
from datetime import datetime,timedelta
import unicodecsv as csv
import sys
import os
import time

if (len(sys.argv) == 2 and sys.argv[1]!="ALL") or (len(sys.argv)==1) or (len(sys.argv) < 3 and sys.argv[1]!="ALL"):
  print "Usage: python get-user-summaries.py ALL \n   or: python generate-daily-summaries.py <start-date-as-yyyy-mm-dd> <end-date-as-yyyy-mm-dd>\n"
  os._exit(-1)

if sys.argv[1]=="ALL":
  find_filter = {}
else:
  start_date = datetime.strptime(sys.argv[1], '%Y-%m-%d')
  end_date = datetime.strptime(sys.argv[2], '%Y-%m-%d') + timedelta(days=1)
  find_filter = {"created_at":{"$gte":start_date,"$lt":end_date}}

def restart_line():
  sys.stdout.write('\r')
  sys.stdout.flush()

def update_user(username, timestamp_of_new_classification, user_ip):
  if username not in user_summaries.keys():
    user_summaries[username] = {}
    user_summaries[username]["first_ever_classification"]=timestamp_of_new_classification
    user_summaries[username]["last_ever_classification"]=timestamp_of_new_classification
  else:
    user_summaries[username]["last_ever_classification"]=timestamp_of_new_classification
  if "user_ips" not in user_summaries[username].keys():
    user_summaries[username]["user_ips"] = [user_ip]
  else:
    if user_ip not in user_summaries[username]["user_ips"]:
      user_summaries[username]["user_ips"].append(user_ip)

print "\nScanning classifications db and generating user summaries..."
# connect to the mongo server
client = pymongo.MongoClient()
db = client['serengeti3']
classification_collection = db["serengeti_classifications"]

# for storing the users seen for each season
user_summaries = {}

# scan through *ALL* classifications (add a .skip or .limit to look at subset)
pageSize = 10000
completed_page_rows=1
skipped = 0

current_day = None
last_classification_created_at = OrderedDict()

for ii, classification in enumerate(classification_collection.find(find_filter,{"created_at":1,"tutorial":1,"user_name":1,"user_ip":1},no_cursor_timeout=True).sort('created_at', pymongo.ASCENDING)):
  completed_page_rows+=1
  if completed_page_rows % 10000 == 0:
    restart_line()
    sys.stdout.write("%s classifications examined..." % completed_page_rows)
    sys.stdout.flush()
  if "tutorial" in classification.keys() and classification["tutorial"]==True:
    skipped += 1
    continue
  else:
    if "user_name" in classification.keys():
      username = classification["user_name"]
      update_user(username, classification["created_at"], classification["user_ip"])

restart_line()
sys.stdout.write("%s classifications processed..." % completed_page_rows)
sys.stdout.flush()

print "\n\nProcessed a total of %s classifications (skipped %s)." % (completed_page_rows,skipped)
print "\nExporting user summaries to CSV..."

wrfile = open("csvs/output/daily-summary/user-summaries.csv", 'w')
writer = csv.writer(wrfile, delimiter=',', quoting=csv.QUOTE_NONNUMERIC,dialect='excel', encoding='utf-8')
writer.writerow(["User ID","Date of First Ever Classification", "Date of Last Ever Classification", "User IPs used"])
for username, user_data in user_summaries.iteritems():
  row = [username, user_data["first_ever_classification"].strftime('%Y-%m-%d'), user_data["last_ever_classification"].strftime('%Y-%m-%d'), '|'.join(user_data["user_ips"])]
  outrow = []
  for el in row:
    if isinstance(el,str):
      outrow.append(unicode(el.decode('utf-8')))
    else:
      outrow.append(el)
  writer.writerow(outrow)
wrfile.close()

print "\nDone.\n"
