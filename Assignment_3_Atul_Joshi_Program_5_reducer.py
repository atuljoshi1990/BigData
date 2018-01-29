#!/usr/bin/env python

import sys

movieRatingNameMap = {}
movieId = None
previousMoviesRating = 0
current_movie = None
movieRatingMap = {}
movieList = []
movieIdNameMap = {}

# input comes from STDIN
for line in sys.stdin:
   # remove leading and trailing whitespace
   line = line.strip()

   movieId, value = line.split('\t', 1)

   if movieId[0:5] == 'rate|':
      var, movieId = movieId.split('|',1)
      # convert count (currently a string) to int
      if current_movie == movieId:
         # if the movieId is not coming for the first time to reducer.
         # take the previous rating from the map
         previousMoviesRating = movieRatingMap[current_movie]
         #compare the previous rating from the current rating
         if value < previousMoviesRating:
            #store the smaller rating in a map.
            movieRatingMap[movieId] = value
      else:
         # if movieId has come to the reducer for the first time.
         movieRatingMap[movieId] = value
         #store the movie in the current movie
         current_movie = movieId
         movieList.append(current_movie)
   if movieId[0:5] == 'name|':
      var, movieId = movieId.split('|',1)
      movieIdNameMap[movieId] = value

for item in movieList:
   #make a map of moviename and all of its ratings
   try:
      name = movieIdNameMap[item]
      if name not in movieRatingNameMap:
         movieRatingNameMap[name] = [movieRatingMap[item]]
      else:
         movieRatingNameMap[name].append(movieRatingMap[item])
   except:
      pass
#sort all the movies in descending order.
a1_sorted_keys = list(reversed(sorted(movieRatingNameMap.keys())))
for item in a1_sorted_keys:
   #group by all movies and sort the ratings in ascending order in case of same movie names.
   newList = movieRatingNameMap[item]
   newList.sort()
   for newItem in newList:
      print '%s\t%s' % (item, newItem)