#!/usr/bin/python

import sys

# Input files will be read and provided to the mapper as input from STDIN

# Read file

for line in sys.stdin:
  # Trim any whitespaces
  line = line.strip()
  results = line.split()
 
  for word in results:
    # Write tab-delimited result to STDOUT in the format '(word, 1)'
    print '%s\t%s' % (word, 1)
