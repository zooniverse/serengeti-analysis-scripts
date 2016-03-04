#!/usr/bin/env python
__author__ = 'alex'
import pymongo
from collections import OrderedDict
from datetime import datetime
import csv
import sys

def restart_line():
  sys.stdout.write('\r')
  sys.stdout.flush()

def get_day_of_classification(timestamp):
  return datetime(*timestamp.timetuple()[:3])

print "Scanning classifications db and generating daily summaries..."
# connect to the mongo server
client = pymongo.MongoClient()
db = client['serengeti3']
classification_collection = db["serengeti_classifications"]

# for storing the users seen for each season
daily_users = OrderedDict()
anon_daily_users_counts = OrderedDict()

# scan through *ALL* classifications (add a .skip or .limit to look at subset)
pageSize = 10000
first_classification = classification_collection.find_one()
completed_page_rows=1
last_id = first_classification["_id"]
last_classification=None
skipped = 0
latest_date=first_classification["created_at"]
current_day = get_day_of_classification(latest_date)

next_results = classification_collection.find({"_id":{"$gt":last_id}},{"created_at":1,"tutorial":1,"user_name":1,"subjects":1},no_cursor_timeout=True).limit(pageSize)
while next_results.count()>0:
  for ii, classification in enumerate(next_results):
    completed_page_rows+=1
    if completed_page_rows % 10000 == 0:
      restart_line()
      sys.stdout.write("%s classifications processed..." % completed_page_rows)
      sys.stdout.flush()
    last_id = classification["_id"]
    latest_date = classification["created_at"]
    date_of_this_classification = get_day_of_classification(latest_date)
    if "tutorial" in classification.keys() and classification["tutorial"]==True:
      skipped += 1
      continue
    else:
      if date_of_this_classification == current_day:
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
      else:
        # start of a new day
        current_day = date_of_this_classification
        print "\nProcessing classifications for %s...\n" % current_day.strftime('%d-%b-%Y')
  next_results = classification_collection.find({"_id":{"$gt":last_id}},{"created_at":1,"tutorial":1,"user_name":1,"subjects":1},no_cursor_timeout=True).limit(pageSize)

restart_line()
sys.stdout.write("%s classifications processed..." % completed_page_rows)
sys.stdout.flush()

print "Processed a total of %s classifications (skipped %s). The latest date examined was %s" % (completed_page_rows,skipped,current_day.strftime('%d-%b-%Y'))
print "Exporting daily user summaries to CSV..."

wrfile = open("daily-users.csv", 'w')
writer = csv.writer(wrfile, delimiter=',', quoting=csv.QUOTE_NONNUMERIC)
writer.writerow(["date","User ID","Classifications"])
for day,user_counts in daily_users.iteritems():
  for user,count in user_counts.iteritems():
    writer.writerow([day.strftime('%Y-%m-%d'),user,count])
wrfile.close()

wrfile = open("daily-anon.csv", 'w')
writer = csv.writer(wrfile, delimiter=',', quoting=csv.QUOTE_NONNUMERIC)
writer.writerow(["date","Anonymous Classifications"])
for day,anon_count in anon_daily_users_counts.iteritems():
    writer.writerow([day.strftime('%Y-%m-%d'),anon_count])
wrfile.close()

print "Done."
