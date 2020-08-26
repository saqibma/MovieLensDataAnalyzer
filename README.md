Spark Based Movie Lens Data Analyzer App

Please go through the following points before running the application.
1) Please run the main program MainApp.scala in order to perform Movie Lens Data Analysis.
2) The main program accepts two arguments 
   a) Full path to the movie lens data. For example D:\movie-lens\ml-1m\.
   b) Full path to the result folder   
3) MovieLensDataAnalyzer performs extraction and transform operations.
   Please refer MovieLensDataAnalyzer.scala file.
6) DataPersistor creates csv and parquet files.
7) MovieLensDataAnalyzer performs following calculations
   i) Calculate Number of Movies Under Each Genres
   ii) Calculate Average Ratings sorted by MovieId
   iii) Calculate Top 100 Movies Based on Average Ratings 
9) Unit testing was done in a TDD style. All the user defined methods were tested thoroughly. The idea behind TDD is to test business logic only.
    We are not supposed to test spark read and write API's as they are well tested.
    Please refer MovieLensDataAnalyzerSpec.scala class.  
10) A generic spark based test framework was designed and developed to test all kinds of RDDs.
    Please refer BaseSuite.scala class for the detail implementation.
14) pom.xml file was updated to create a jar with dependencies called uber jar.

