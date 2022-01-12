from pyshark.sql import SparkSession
from pyshark.sql import Row
from pyshark.sql import functions

def loadMovieNames():
    movieNames = {}
    with open('ml-100k/u.item') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
        return movieNames

def parseInput(line):
    fields = line.split()
    return Row(movieID = int(fields[1]), rating = float(fields[2]))

if __name__ == "__main__":
    spark = SparkSession.builder.appName("PopularMovies").getOrCreate()
    # load up our movie ID -> name dict
    movieNames = loadMovieNames()
    # get the row data
    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.data")
    movies = lines.map(parseInput)
    movieDataset = spark.createDataFrame(movies)
    averageRatings = movieDataset.groupBy("movieID").avg("rating")
    counts = movieDataset.groupBy("movieID").count()
    # Join the two together
    averagesAndCounts = counts.join(averageRatings, "movieID")
    # pull the top 10 results
    topTen = averagesAndCounts.orderBy("avg(rating)").take(10)
    for movie in topTen:
        print(movieNames[movie[0]], movie[1], movie[2])
    spark.stop()