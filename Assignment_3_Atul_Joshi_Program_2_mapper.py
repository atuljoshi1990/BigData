#!/usr/bin/env python

import sys

try:
    for line in sys.stdin:
        line = line.strip()
        words = line.split("::")
        #pass each movieId and its rating to the reducer.
        print '%s\t%s' % (words[1], words[2])
except:
    pass