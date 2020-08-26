package com.asos.sparktest

class MovieLensDataAnalyzerSpec extends BaseSuite {
  private[this] var movieLensDataAnalyzer: MovieLensDataAnalyzer = _

  override def beforeAll() {
    super.beforeAll()
    movieLensDataAnalyzer = new MovieLensDataAnalyzer(sc)
  }

  test("calculate average ratings") {
    //Given
    //UserID::MovieID::Rating::Timestamp
    val ratingsData = Seq(
      "1::1193::5::978300760",
      "2::1193::4::978302109",
      "3::1193::3::978301968")
    val ratingsRDD = sc.parallelize(ratingsData)

    //when
    val averageRatingsRDD = movieLensDataAnalyzer.calculateAverageRatings(ratingsRDD)

    //Then
    val expectedAverageRatingsRDD = sc.parallelize(Seq((1193, 3, 4.0)))
    assertRDDEquals(expectedAverageRatingsRDD, averageRatingsRDD){
      //Comparing elements from the expected and calculated RDDs
      (rec1: (Int, Int, Double), rec2: (Int, Int, Double)) =>
        rec1._1 == rec2._1 && rec1._2 == rec2._2 && rec1._3 == rec2._3
    }
  }

  test("calculate number of movies under each genres") {
    //Given
    //UserID::MovieID::Rating::Timestamp
    val moviesData = Seq(
      (1, ("Toy Story (1995)", "Animation|Children's|Comedy")),
      (2, ("Jumanji (1995)", "Adventure|Children's|Fantasy")),
      (3, ("Grumpier Old Men (1995)", "Comedy|Romance")),
      (4, ("Waiting to Exhale (1995)", "Comedy|Drama")),
      (5, ("Father of the Bride Part II (1995)", "Animation|Children's|Comedy")));

    val moviesRDD = sc.parallelize(moviesData)

    //when
    val numberOfMoviesUnderEachGenresRDD = movieLensDataAnalyzer.calculateNumberOfMoviesUnderEachGenres(moviesRDD)

    //Then
    val expectedNumberOfMoviesUnderEachGenresRDD = sc.parallelize(Seq(
      ("Adventure",1),
      ("Animation", 2),
      ("Children's", 3),
      ("Comedy", 4),
      ("Drama", 1),
      ("Fantasy", 1),
      ("Romance", 1)))

    assertRDDEquals(expectedNumberOfMoviesUnderEachGenresRDD, numberOfMoviesUnderEachGenresRDD){
      //Comparing elements from the expected and calculated RDDs
      (rec1: (String, Int), rec2: (String, Int)) =>
        rec1._1 == rec2._1 && rec1._2 == rec2._2
    }
  }

  override def afterAll() {
    sc.stop()
    // Clearing the driver port so that we don't try and bind to the same port on restart.
    System.clearProperty("spark.driver.port")
    super.afterAll()
  }
}
