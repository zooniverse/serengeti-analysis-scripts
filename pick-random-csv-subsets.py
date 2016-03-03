import csv
import random
import subprocess
import os
import sys
import numpy

def file_len(fname):
  p = subprocess.Popen(['wc', '-l', fname], stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE)
  result, err = p.communicate()
  if p.returncode != 0:
      raise IOError(err)
  return int(result.strip().split()[0])

def create_csv(csv_directory_name, csv_filename):
  if not os.path.exists(csv_directory_name):
    os.makedirs(csv_directory_name)
  wrfile = open("%s/%s" % (csv_directory_name, csv_filename), 'w')
  writer = csv.writer(wrfile, delimiter=',', quoting=csv.QUOTE_NONNUMERIC)
  writer.writerow(["url","Subject ID","Frame","Season","Site","Roll","Decision Type","Crowd Determination"])
  return {"handle": wrfile, "writer": writer}

species_list = ["elephant","ostrich","buffalo","warthog","wildebeest","blank"]

rows_needed = int(sys.argv[1])

csvwriters = {}
for species in species_list:
  csvwriters[species] = create_csv("csvs/output/sets", "%s-%s.csv" % (species,rows_needed))

if len(sys.argv) < 2:
  print "Usage: python pick-random-csv-subsets.py <rows-needed>"
  os._exit(-1)
else:
  print "\nLoading CSVs and picking sets per species. Current sets are %s:\n" % species_list
  for species in species_list:
    if not os.path.exists("csvs/input/consensus-detailed.csv"):
      print "Missing input file \"csvs/output/consensus-detailed.csv\".\nSkipping species."
      os._exit(-1)

    rows_available = int(file_len('csvs/output/%s.csv' % species))

    picked_row_indices = numpy.random.choice(rows_available,replace=False,size=rows_needed)
    with open('csvs/output/%s.csv' % species, 'rb') as csvfile:
      rows_written = 0
      reader = csv.reader(csvfile, delimiter=',')
      next(reader)
      i=0
      for row in reader:
        i += 1
        if i in picked_row_indices:
          csvwriters[species]["writer"].writerow(row)
          rows_written += 1
    print "For species '%s', wrote %s of %s available rows to a new CSV file." % (species,rows_written,rows_available)

print "Done.\n"