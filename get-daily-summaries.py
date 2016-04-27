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
  print "Usage: python get-daily-summaries.py ALL \n   or: python generate-daily-summaries.py <start-date-as-yyyy-mm-dd> <end-date-as-yyyy-mm-dd>\n"
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

def get_previous_day(timestamp):
  try:
    return timestamp - timedelta(days=1)
  except KeyError:
    return None

def is_within_same_session(previous_timestamp, this_timestamp):
  try:
    return (this_timestamp - previous_timestamp).seconds < 30 * 60
    # note, session length currently set to 30 minutes. Change first number to 10 for a 10 min session
  except KeyError:
    return None

def get_yesterdays_last_session(username, timestamp_of_new_classification):
  current_day = get_day_of_classification(timestamp_of_new_classification)
  yesterday = get_previous_day(current_day)
  if yesterday in last_classification_created_at.keys() and username in last_classification_created_at[yesterday].keys():
    yesterdays_last_session_number_by_this_user = next(reversed(daily_users[yesterday][username]))
    yesterdays_last_classification_by_this_user = last_classification_created_at[yesterday][username]
    return {
      "yesterday": yesterday,
      "session_number": yesterdays_last_session_number_by_this_user,
      "last_classification_time": yesterdays_last_classification_by_this_user,
      "still_active": is_within_same_session(yesterdays_last_classification_by_this_user, timestamp_of_new_classification)
    }
  else:
    return None

def initialise_daily_user_records(day, username=None, session_number=None):
  if day not in daily_users.keys():
    daily_users[day] = OrderedDict()
  if username is not None and username not in daily_users[current_day].keys():
    daily_users[day][username] = OrderedDict()
  if session_number is not None and username is not None and session_number not in daily_users[day][username].keys():
    daily_users[day][username][session_number] = OrderedDict()

def initialise_last_classification_records(username, timestamp_of_new_classification):
  current_day = get_day_of_classification(timestamp_of_new_classification)
  if current_day not in last_classification_created_at.keys():
    last_classification_created_at[current_day] = OrderedDict()
  if username is not None and username not in last_classification_created_at[current_day].keys():
    last_classification_created_at[current_day][username] = timestamp_of_new_classification

def add_this_to_yesterdays_last_session(username, yest_last, timestamp_of_new_classification):
  last_classification_created_at[yest_last["yesterday"]][username] = timestamp_of_new_classification
  daily_users[yest_last["yesterday"]][username][yest_last["session_number"]]["last_classification_time"] = timestamp_of_new_classification
  daily_users[yest_last["yesterday"]][username][yest_last["session_number"]]["classification_count"] += 1

def add_this_to_todays_latest_session(username, timestamp_of_new_classification):
  current_day = get_day_of_classification(timestamp_of_new_classification)
  if len(daily_users[current_day][username].keys())==0:
    current_session_number = 0
  else:
    current_session_number = next(reversed(daily_users[current_day][username]))
  last_classification_created_at[current_day][username] = timestamp_of_new_classification
  daily_users[current_day][username][current_session_number]["last_classification_time"] = classification["created_at"]
  daily_users[current_day][username][current_session_number]["classification_count"] += 1

def add_this_to_a_new_session_today(username, timestamp_of_new_classification, user_ip):
  current_day = get_day_of_classification(timestamp_of_new_classification)
  if len(daily_users[current_day][username])==0:
    current_session_number = 0
  else:
    current_session_number = next(reversed(daily_users[current_day][username]))
  new_session_number = current_session_number + 1
  last_classification_created_at[current_day][username] = timestamp_of_new_classification
  daily_users[current_day][username][new_session_number] = OrderedDict()
  daily_users[current_day][username][new_session_number]["first_classification"] = classification["created_at"]
  daily_users[current_day][username][new_session_number]["classification_count"] = 1
  daily_users[current_day][username][new_session_number]["user_ip"] = user_ip

def store_this_classification(username, current_day, timestamp_of_new_classification, user_ip):
  is_new_session = False
  if current_day not in last_classification_created_at.keys() or username not in last_classification_created_at[current_day].keys():
    # first time encountering this user on this day - need to check if user was working right up to midnight yesterday
    yest_last = get_yesterdays_last_session(username, current_day)
    if yest_last is not None and yest_last["still_active"]:
      # we are still continuing yesterday's last session
      add_this_to_yesterdays_last_session(username, yest_last, timestamp_of_new_classification)
    else:
      initialise_last_classification_records(username, timestamp_of_new_classification)
      is_new_session = True
  else:
    # normal case, not continuing from yesterday and not a first encounter today - just check against this user's earlier classification today
    if is_within_same_session(last_classification_created_at[current_day][username],timestamp_of_new_classification):
      # extension of previous session - update the session data with a new classification
      add_this_to_todays_latest_session(username, timestamp_of_new_classification)
    else:
      is_new_session = True
  if is_new_session:
    add_this_to_a_new_session_today(username, timestamp_of_new_classification, user_ip)
  return is_new_session

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
last_classification_created_at = OrderedDict()

for ii, classification in enumerate(classification_collection.find(find_filter,{"created_at":1,"tutorial":1,"user_name":1,"user_ip":1,"subjects":1},no_cursor_timeout=True).sort('created_at', pymongo.ASCENDING)):
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
      username = classification["user_name"]
      initialise_daily_user_records(current_day,username)
      store_this_classification(username, current_day, classification["created_at"], classification["user_ip"])
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

wrfile = open("csvs/output/daily-summary/daily-users.csv", 'w')
writer = csv.writer(wrfile, delimiter=',', quoting=csv.QUOTE_NONNUMERIC,dialect='excel', encoding='utf-8')
writer.writerow(["date","User IP","User ID","Session Number","First Classification","Last Classification", "Classifications in Session"])
for day,users_sessions in daily_users.iteritems():
  for user,sessions in users_sessions.iteritems():
    for session_number,session in sessions.iteritems():
      if "last_classification_time" not in session.keys():
        # fix dangling sessions which only had one classification
        session["last_classification_time"] = session["first_classification"]
      row = [day.strftime('%Y-%m-%d'), session["user_ip"], user, session_number, session["first_classification"].strftime('%Y-%m-%d %H:%M:%S'), session["last_classification_time"].strftime('%Y-%m-%d %H:%M:%S'), session["classification_count"]]
      outrow = []
      for el in row:
        if isinstance(el,str):
          outrow.append(unicode(el.decode('utf-8')))
        else:
          outrow.append(el)
      writer.writerow(outrow)
wrfile.close()

wrfile = open("csvs/output/daily-summary/daily-anon.csv", 'w')
writer = csv.writer(wrfile, delimiter=',', quoting=csv.QUOTE_NONNUMERIC,dialect='excel', encoding='utf-8')
writer.writerow(["date","Anonymous Classifications"])
for day,anon_count in anon_daily_users_counts.iteritems():
  row = [day.strftime('%Y-%m-%d'),anon_count]
  outrow = []
  for el in row:
    if isinstance(el,str):
      outrow.append(unicode(el.decode('utf-8')))
    else:
      outrow.append(el)
  writer.writerow(outrow)
wrfile.close()

print "\nDone.\n"
