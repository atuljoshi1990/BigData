#!/usr/bin/env python

from operator import itemgetter
import sys
import math

current_rating_count = 0
totalRating = 0
movieId = None
current_movie = None
uniqueRatingCount = 0
totalVariance = 0
std = 0
avg = 0
movieRatingMap = {}
totalNumberOfRatingMap = {}
count = 0
avgSqr = 0
movie = None
finalCountMap = {}
muvLst = []
avgMap = {}
movieRatingList = []
try:
    # input comes from STDIN
    for line in sys.stdin:
        # remove leading and trailing whitespace
        line = line.strip()

        # parse the input we got from mapper.py
        movieId, rating = line.split('\t', 1)
        movieRatingList.append(movieId+":"+rating)
        rating = float( rating )

        if current_movie == movieId:
            #if the movieId is not coming for the first time to reducer.
            #take the previous rating from the map
            current_rating_count = movieRatingMap[current_movie]
            #add the current rating to the previous rating.
            current_rating_count += rating
            #save the total updated rating.
            movieRatingMap[current_movie] = current_rating_count
            #update the total count of the number of times a movie is rated.
            totalRating = totalNumberOfRatingMap[current_movie]
            totalRating = float(totalRating + 1)
            totalNumberOfRatingMap[current_movie] = totalRating
        else:
            #if movieId has come to the reducer for the first time.
            totalRating = float( 0 );
            #assign the rating to current rating.
            current_rating_count = rating
            #assign movie to current movie
            current_movie = movieId
            #total number of times the same movie has come.
            totalRating = float(totalRating + 1)
            #store the total ratings for a single movie and the total number times a movie is rated to two different maps.
            totalNumberOfRatingMap[current_movie] = totalRating
            movieRatingMap[current_movie] = current_rating_count
    new_Movie = None
	#for calculating standard deviation for each movie's total rating.
	for item in movieRatingList:
		item = item.strip( )
		movie, movieRating = item.split(':', 1 )
		movieRating = float(movieRating)
		if new_Movie == movie:
			#take values from the maps set previously and calulate the average rating for a movie.
			avg = float( movieRatingMap[new_Movie] ) / float( totalNumberOfRatingMap[new_Movie] )
			avgSqr = avgMap[new_Movie] + ((movieRating - avg) ** 2)
			#store the avegare of the square rating for a movie in a map.
			avgMap[new_Movie] = avgSqr
		else:
			muvLst.append(movie)
			new_Movie = movie
			# take values from the maps set previously and calulate the average rating for a movie.
			avg = float( movieRatingMap[movie] ) / float( totalNumberOfRatingMap[movie] )
			avgSqr = (movieRating - avg) ** 2
			# store the avegare of the square rating for a movie in a map.
			avgMap[new_Movie] = avgSqr
	for item in muvLst:
		#calculate the standard deviation by dividing the average sqauare of rating by total number of times a muv is rated.
		std = float(avgMap[item])/float(totalNumberOfRatingMap[item])
		print '%s\t%s' % (item, math.sqrt( std ))
except:
    pass



