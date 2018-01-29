#!/usr/bin/env python

import sys

cacheFile = open('movies.dat', 'r')
movieId = None
currentMovieRating = 0
muvId = None
muvName = None
#get the input from standard input file.
try:
    for key in sys.stdin:
        key = key.strip()
        # parse the input we got from mapper.py
        words = key.split("::")
        currentMovieRating = words[2]
        movieId = words[1]
        currentMovieRating = int(currentMovieRating)
        #checks for invalid movie rating and invalid movieId
        if currentMovieRating > 1 and currentMovieRating < 6:
            if int(movieId) > 1 and int(movieId) < 3953:
				print '%s\t%s' % ("rate|"+movieId, currentMovieRating)
    #get the input from cache.
    for line in cacheFile:
        line = line.strip()
        wordss = line.split("::")
        muvId = wordss[0]
        muvName = wordss[1]
        if int(muvId) > 1 and int(muvId) < 3953:
            print '%s\t%s' % ("name|"+muvId, muvName)
except:
    pass

