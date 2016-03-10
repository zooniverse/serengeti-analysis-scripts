import csv
from bson.objectid import ObjectId
import os
import sys

csvwriters = {}
counts = {}

MAX_ANIMALS_PER_IMAGE=5

def restart_line():
  sys.stdout.write('\r')
  sys.stdout.flush()

def create_csv(csv_directory_name, csv_filename):
  if not os.path.exists(csv_directory_name):
    os.makedirs(csv_directory_name)
  wrfile = open("%s/%s" % (csv_directory_name, csv_filename), 'w')
  writer = csv.writer(wrfile, delimiter=',', quoting=csv.QUOTE_NONNUMERIC)
  writer.writerow(["url","Subject ID","Frame","Season","Site","Roll","Decision Type","Crowd Determination","Number of Species Present","Number of Animals Present"])
  return {"handle": wrfile, "writer": writer}

def retire_reason_explain(retire_reason):
  if retire_reason=="complete":
    return "Most common opinion from 25 opinions"
  elif retire_reason=="consensus":
    return "10 opinions agree upon the species present"
  elif retire_reason=="blank_consensus":
    return "10 opinions agree this is blank"
  elif retire_reason=="blank":
    return "First 5 opinions were blank"

def nicefy_species(species):
  if species=="blank":
    return "No animals present"
  elif species=="elephant":
    return "Elephants"
  elif species=="buffalo":
    return "Buffalo"
  elif species=="ostrich":
    return "Ostriches"
  elif species=="warthog":
    return "Warthogs"
  elif species=="gazellethomsons":
    return "Thomson's Gazelles"
  elif species=="gazellegrants":
    return "Grant's Gazelles"
  elif species=="guineafowl":
    return "Guinea Fowls"
  elif species=="zebra":
    return "Zebras"
  elif species=="hartebeest":
    return "Hartebeest"
  elif species=="multi":
    return "Multiple species present"
  elif species=="wildebeest":
    return "Wildebeest"
  #TODO if any other species used, need to add them to this array for a nicer output
  else:
    return species

def add_images_to_csv_for(subject, csvwriter):
  if subject["retire_reason"]!="unretired":
    frame_no = 0
    species = subject["crowd_says"]
    if species not in csvwriters.keys():
      csvwriters[species] = create_csv("csvs/output/species-full", "%s.csv" % species)
      counts[species]=0
    csvwriter = csvwriters[species]["writer"]
    for this_url in subject["frame_urls"]:
      csvwriter.writerow([this_url,subject["subject_id"],(frame_no+1),subject["season"],subject["site_id"],subject["roll_code"],retire_reason_explain(subject["retire_reason"]),nicefy_species(subject["crowd_says"]),subject["total_species"],subject["total_animals"]])
      frame_no += 1

def get_season_no_from_char(c):
  if c=='0':
    return 9 # Season 9
  elif c=='9':
    return 0 # Lost Season
  elif c=='A':
    return 10 # Season 10
  else:
    return int(c)

if len(sys.argv) < 2:
  print "Usage: python generate-species-csvs.py <all|season-numbers-without-spaces>"
  print "(Note that Lost Season is represented by '9', season 9 by '0', season 10 by 'A')\n"
  os._exit(-1)
else:
  if not os.path.exists("csvs/input/consensus-detailed.csv"):
    print "\nMissing input file \"csvs/input/consensus-detailed.csv\".\nPlease run \"load 'generate_detailed_consensus.rb'\" from an Ouroboros Rails console\n(started with \"RAILS_ENV=staging bundle exec rails c\").\n"
    os._exit(-1)
  seasons_string = str(sys.argv[1])
  if seasons_string == "all":
    seasons_string = "123456789A"
  seasons_to_include = []
  for season_char in list(seasons_string):
    seasons_to_include.append(get_season_no_from_char(season_char))

print "\nLoading subject data from CSV:"

# load subject data from CSV
subjects_index = {}
with open('csvs/input/consensus-detailed.csv', 'rb') as csvfile:
  reader = csv.reader(csvfile, delimiter=',', quotechar='"')
  next(reader) # skip header line
  i=0
  for row in reader:
    i += 1
    if i % 5000 == 0:
      restart_line()
      sys.stdout.write("%s subjects loaded..." % i)
      sys.stdout.flush()
    subject = {}
    subject_id = row[0]
    subject["subject_id"]=subject_id
    if row[1]:
      subject["season"] = int(row[1])
    else:
      subject["season"] = "N/A"
    if row[2]:
      subject["site_id"] = row[2]
    else:
      subject["site_id"] = "N/A"
    if row[3]:
      subject["roll_code"] = row[3]
    else:
      subject["roll_code"] = "N/A"
    subject["no_of_frames"]=int(row[4])
    if subject["no_of_frames"]==3:
      subject["frame_urls"] = [row[5],row[6],row[7]]
    elif subject["no_of_frames"]==2:
      subject["frame_urls"]=[row[5],row[6]]
    else:
      subject["frame_urls"]=[row[5]]
    subject["crowd_says"] = row[10]
    if row[11]:
      subject["total_species"] = row[11]
    else:
      subject["total_species"] = 0
    if row[12]:
      subject["total_animals"] = row[12]
    else:
      subject["total_animals"] = 0
    if row[14]:
      subject["retire_reason"] = row[14]
    else:
      subject["retire_reason"] = "unretired"
    subjects_index[subject_id] = subject

print "\n\nProcessing subjects and writing CSVs:"

i = 0
for subject_id,subject in subjects_index.iteritems():
  i += 1
  if i % 5000 == 0:
    restart_line()
    sys.stdout.write("%s subjects written to CSV..." % i)
    sys.stdout.flush()
  if subject["total_animals"] <= MAX_ANIMALS_PER_IMAGE:
    add_images_to_csv_for(subject, csvwriters)

print "\n\nClosing CSV handles...\n"

for species,csvwriter in csvwriters.iteritems():
  csvwriter["handle"].close()

print "Done.\n"