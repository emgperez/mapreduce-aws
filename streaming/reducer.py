#!/usr/bin/python

import sys

key = None
currentKey = None
currentCount = 0

# The reducer receives its input (through STDIN) from the mapper output

for line in sys.stdin:
  # Trim whitespaces
  line =  line.strip()
  key, count = line.split('\t', 1)

  try:
    count = int(count)
  except ValueError:
    continue

  # Since all the keys are sorted before getting to the reducer, we increase the counter of a given key until we find a different key
  if currentKey == key:
    currentCount += count
  else:
    if currentKey:
      # Write the results to STDOUT
      print '%s,%s' % (currentKey, currentCount)
    
    currentCount = count
    currentKey = key

# This section makes sure the last key is also counted
if currentKey == key:
   print '%s,%s' % (currentKey, currentCount)
    
