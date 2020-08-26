package com.asos.sparktest

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class MovieLensDataAnalyzer(@transient val context: SparkContext) extends Serializable {

  private val DOUBLE_COLON: String = "::"

  def extract(input: Map[String, String]): Map[String, RDD[String]] = {

    //Reading data from movies.dat
    val moviesData = context.textFile(input.get("MOVIES").get)

    //Reading data from ratings.dat
    val ratingsData = context.textFile(input.get("RATINGS").get)

    val extractedRDD = Map.newBuilder[String, RDD[String]]
    extractedRDD.+=("MOVIES_RDD" -> moviesData)
    extractedRDD.+=("RATINGS_RDD" -> ratingsData)
    extractedRDD.result()
  }

  def transformAndPersist(extracted: Map[String, RDD[String]], resultPath: String): Unit = {
    val moviesData = extracted.get("MOVIES_RDD").get
    val ratingsData = extracted.get("RATINGS_RDD").get

    //Creating RDD for the tuple(MovieID, (Title, Genres)).
    val moviesRDD = moviesData.map { moviesRecord =>
      val movieFields = moviesRecord.split(DOUBLE_COLON)
      (movieFields(0).trim.toInt, (movieFields(1).trim, movieFields(2).trim))
    }

    moviesRDD.persist()

    val averageRatingsRDD = calculateAverageRatings(ratingsData)

    averageRatingsRDD.persist()

    DataPersistor.createCSV(
      averageRatingsRDD
        .collect()
        .map(rec => s"${rec._1}, ${rec._2}, ${rec._3}"),
      s"${resultPath}/averageRatings.csv")

    val numberOfMoviesUnderEachGenresRDD = calculateNumberOfMoviesUnderEachGenres(moviesRDD)
    DataPersistor.createCSV(
      numberOfMoviesUnderEachGenresRDD
        .collect()
        .map(rec => s"${rec._1}, ${rec._2}"),
      s"${resultPath}/numberOfMoviesUnderEachGenres.csv")

    val moviesBasedOnTheirRatings = calculateTop100MoviesBasedOnAverageRatings(averageRatingsRDD, moviesRDD)
    DataPersistor.createParquet(
      context,
      moviesBasedOnTheirRatings,
      s"${resultPath}/top100Movies")
  }

  def calculateNumberOfMoviesUnderEachGenres(moviesRDD: RDD[(Int, (String, String))]): RDD[(String, Int)] =
    moviesRDD.flatMap { rec =>
      val genres = rec._2._2.split('|')
      genres.map(genre => (genre, 1))
    }.reduceByKey(_ + _)
      .sortBy(_._1)

  def calculateAverageRatings(ratingsData: RDD[String]) = {
    //Creating RDD for the tuple(MovieID, (Rating, 1)).
    val ratingsRDD = ratingsData.map { ratingsRecord =>
      val ratingsFields = ratingsRecord.split(DOUBLE_COLON)
      (ratingsFields(1).trim.toInt, (ratingsFields(2).trim.toDouble, 1))
    }

    //Calculating Average Ratings and creating tuples(MovieId, No of Users, Average Rating).
    ratingsRDD.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._1, x._2._2, x._2._1 / x._2._2))
      .sortBy(_._1) // sorted by MovieId
  }

  private def calculateTop100MoviesBasedOnAverageRatings(averageRatingsRDD: RDD[(Int, Int, Double)], moviesRDD: RDD[(Int, (String, String))]) = {
    val movieIdAverageRatingsPairedRDD = averageRatingsRDD.map(rec => (rec._1, rec._3))
    val movieIdTitlePairedRDD = moviesRDD.map { rec => (rec._1, rec._2._1) }
    val rank = context.longAccumulator
    val moviesBasedOnTheirRatings = movieIdTitlePairedRDD.join(movieIdAverageRatingsPairedRDD)
      .sortBy(_._2._2, false)
      .map { case (movieId, (title, avgRating)) => rank.add(1L)
        (rank.value.toLong, movieId, title, avgRating)
      }
    moviesBasedOnTheirRatings
  }
}
