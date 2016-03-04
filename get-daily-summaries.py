#!/usr/bin/env python
__author__ = 'alex'
import pymongo
from collections import OrderedDict
from datetime import datetime,timedelta
import csv
import sys
import os
import time

if (len(sys.argv) == 2 and sys.argv[1]!="ALL") or (len(sys.argv) < 3 and sys.argv[1]!="ALL"):
  print "Usage: python generate-daily-summaries.py ALL \n   or: python generate-daily-summaries.py <start-date-as-yyyy-mm-dd> <end-date-as-yyyy-mm-dd>\n"
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

def get_day_of_classification(timestamp):
  return datetime(*timestamp.timetuple()[:3])

print "\nScanning classifications db and generating daily summaries..."
# connect to the mongo server
client = pymongo.MongoClient()
db = client['serengeti3']
classification_collection = db["serengeti_classifications"]

# for storing the users seen for each season
daily_users = OrderedDict()
anon_daily_users_counts = OrderedDict()

# scan through *ALL* classifications (add a .skip or .limit to look at subset)
pageSize = 10000
completed_page_rows=1
skipped = 0

current_day = None

for ii, classification in enumerate(classification_collection.find(find_filter,{"created_at":1,"tutorial":1,"user_name":1,"subjects":1},no_cursor_timeout=True).sort('created_at', pymongo.ASCENDING)):
  completed_page_rows+=1
  if completed_page_rows % 10000 == 0:
    restart_line()
    sys.stdout.write("%s classifications examined..." % completed_page_rows)
    sys.stdout.flush()
  date_of_this_classification = get_day_of_classification(classification["created_at"])
  if "tutorial" in classification.keys() and classification["tutorial"]==True:
    skipped += 1
    continue
  else:
    if date_of_this_classification != current_day:
      if current_day in daily_users.keys() and current_day in anon_daily_users_counts.keys():
        print "\n\nLogged %s daily users for %s (and %s anonymous too)." % (len(daily_users[current_day]),current_day.strftime('%d-%b-%Y'),anon_daily_users_counts[current_day])
      # start of a new day
      current_day = date_of_this_classification
      print "\nProcessing classifications for %s...\n" % current_day.strftime('%d-%b-%Y')
    subject_id = classification["subjects"][0]["zooniverse_id"]
    if "user_name" in classification.keys():
      user_name = classification["user_name"]
      if not current_day in daily_users.keys():
        daily_users[current_day]=OrderedDict()
      if not user_name in daily_users[current_day].keys():
        daily_users[current_day][user_name]= 0
      daily_users[current_day][user_name] += 1
    else:
      if not current_day in anon_daily_users_counts.keys():
        anon_daily_users_counts[current_day]=0
      anon_daily_users_counts[current_day] += 1

restart_line()
sys.stdout.write("%s classifications processed..." % completed_page_rows)
sys.stdout.flush()

if current_day in daily_users.keys() and current_day in anon_daily_users_counts.keys():
  print "\n\nLogged %s daily users for %s (and %s anonymous too)." % (len(daily_users[current_day]),current_day.strftime('%d-%b-%Y'),anon_daily_users_counts[current_day])

print "\n\nProcessed a total of %s classifications (skipped %s). The latest date examined was %s" % (completed_page_rows,skipped,current_day.strftime('%d-%b-%Y'))
print "\nExporting daily user summaries to CSV..."

wrfile = open("csvs/output/daily-users.csv", 'w')
writer = csv.writer(wrfile, delimiter=',', quoting=csv.QUOTE_NONNUMERIC)
writer.writerow(["date","User ID","Classifications"])
for day,user_counts in daily_users.iteritems():
  for user,count in user_counts.iteritems():
    writer.writerow([day.strftime('%Y-%m-%d'),user,count])
wrfile.close()

wrfile = open("csvs/output/daily-anon.csv", 'w')
writer = csv.writer(wrfile, delimiter=',', quoting=csv.QUOTE_NONNUMERIC)
writer.writerow(["date","Anonymous Classifications"])
for day,anon_count in anon_daily_users_counts.iteritems():
    writer.writerow([day.strftime('%Y-%m-%d'),anon_count])
wrfile.close()

print "\nDone.\n"
