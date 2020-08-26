package com.asos.sparktest

import java.io.{File, IOException, PrintWriter}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

object DataPersistor {

  def createCSV(data: Array[String], path: String): Unit = {
    val csvOutputFile = new File(path)
    val printWriter = new PrintWriter(csvOutputFile)
    try {
      data.foreach(printWriter.println)
    } catch {
      case e: IOException => //log error message
    } finally {
      printWriter.close()
    }
  }

  def createParquet(context: SparkContext, transformed: RDD[(Long, Int, String, Double)], path: String): Unit = {
    context.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    val spark: SparkSession = SparkSession.builder().appName(context.appName).master(context.master).getOrCreate()
    val transformedDf = spark.createDataFrame(transformed)

    transformedDf
      .limit(100)
      .coalesce(1)
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("header","false")
      .save(path)
  }
}
