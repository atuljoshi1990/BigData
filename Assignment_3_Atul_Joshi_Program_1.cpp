#define _GLIBCXX_USE_CXX11_ABI 0
#include <algorithm>
#include <limits>
#include <string>
#include "stdint.h"
#include "Pipes.hh"
#include "TemplateFactory.hh"
#include "SerialUtils.hh"
#include "StringUtils.hh"
#include <iomanip> 
#include <sstream> 
#include <map> 

  class MoviesPerGenreMapper: public HadoopPipes::Mapper {
    public: MoviesPerGenreMapper(HadoopPipes::TaskContext & context) {}
      // Map function: Receives a line, outputs ("1", word) to reducer.
    void map(HadoopPipes::MapContext & context) {
      // Get line of text
      std::string line = context.getInputValue();
      // Split into words
      std::vector < std::string > words = HadoopUtils::splitString(line, "::");
      std::vector < std::string > genres = HadoopUtils::splitString(words[2], "\\|");
      if (words[2].at(0) != ' ')
        for (unsigned int i = 0; i < genres.size(); i++)
          context.emit(HadoopUtils::toString(1), genres[i]);

    }
  };

class MoviesPerGenreReducer: public HadoopPipes::Reducer {
  public: MoviesPerGenreReducer(HadoopPipes::TaskContext & context) {}
    // Reduce function
  void reduce(HadoopPipes::ReduceContext & context) {
    float count =0.00;
    std::map < std::string, int > genresMap;
    // Get all tuples with the same value, and count their numbers and stores them in a map.
    while (context.nextValue()) {
      std::map < std::string, int > ::iterator it = genresMap.find(context.getInputValue());
      if (it != genresMap.end()) {
		  //if te genre is already present in the map then increase the count of it and update the map.
        genresMap[context.getInputValue()] = genresMap[context.getInputValue()] + 1;
      } else {
		  //if the genre is not present in the map add it and the count.
        genresMap[context.getInputValue()] = 1;
      }
      count += 1;
    }
    float avg = 0.00;
    std::map < std::string, int > ::iterator itr;
    for (itr = genresMap.begin(); itr != genresMap.end(); itr++) {
      //calculate the average 
	  avg = genresMap[itr -> first] / count;
      //type float was not able to convert to type string using toString method.
	  //so used stringstream for that
	  std::stringstream sm;
      sm << std::fixed << std::setprecision(2) << avg;
      std::string stringNum = sm.str();
      context.emit(itr -> first,stringNum);
    }
  }
};

int main(int argc, char *argv[]) {
  return HadoopPipes::runTask(
    HadoopPipes::TemplateFactory <MoviesPerGenreMapper, MoviesPerGenreReducer> ()
  );
}
