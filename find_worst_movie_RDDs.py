from pyspark import SparkConf, SparkContext

def loadMovieNames():
    movieNames = {}
    with open('ml-100k/u.item') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
        return movieNames

def parseInput(line):
    fields = line.split()
    return (int(fields[1]), (float(fields[2]), 1.0))

if __name__ == '__main__':
    # create the SparkContext
    conf = SparkConf().setAppName("WorstMovies")
    sc = SparkContext(conf = conf)
    # load up our movie ID -> movie name lookup table
    movieNames = loadMovieNames()
    # load uo the row u.data file
    lines = sc.textFile("hdfs:///user/maria_dev/ml-100k/u.data")
    movieRatings = lines.map(parseInput)
    ratingTotalsAndCount = movieRatings.reduceByKey(lambda movie1, movie2: (movie1[0] + movie2[0]))
    averageRatings = ratingTotalsAndCount.mapValues(lambda totalAndCount : totalAndCount[0]/totalAndCount[1])
    sortedMovies = averageRatings.sortBy(lambda x: x[1])
    # take the top 10 rated movies
    results = sortedMovies.take(10)