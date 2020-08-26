package com.asos.sparktest

import org.apache.spark.{SparkConf, SparkContext}

object MainApp extends App {

  val appName = "movielens-data-analyzer"
  val master = "local[*]"
  val conf: SparkConf = new SparkConf().setAppName(appName).setMaster(master)
  conf.set("spark.sql.shuffle.partitions","2")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.set("spark.testing.memory", "2147480000")
  val sc = new SparkContext(conf)

  val movieLensDataPath: String = args(0)
  val resultsOutputPath: String = args(1)

  val input = Map.newBuilder[String, String]
  input.+=("MOVIES" -> s"${movieLensDataPath}/movies.dat")
  input.+=("RATINGS" -> s"${movieLensDataPath}/ratings.dat")
  input.+=("USERS" -> s"${movieLensDataPath}/users.dat")

  val movieLensDataAnalyzer = new MovieLensDataAnalyzer(sc)

  val movieLensDataExtracted = movieLensDataAnalyzer.extract(input.result())

  movieLensDataAnalyzer.transformAndPersist(movieLensDataExtracted, resultsOutputPath)
}
