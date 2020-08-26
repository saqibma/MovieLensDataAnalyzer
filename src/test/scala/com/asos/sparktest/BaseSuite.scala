package com.asos.sparktest

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, GivenWhenThen}

import scala.reflect.ClassTag

abstract class BaseSuite extends FunSuite with BeforeAndAfterAll with GivenWhenThen {
  @transient protected var _sc: SparkContext = _
  protected def sc: SparkContext = _sc

  override def beforeAll() {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("test")
      .set("spark.sql.shuffle.partitions","2")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.testing.memory", "2147480000")
    _sc = new SparkContext(conf)
  }

  protected def assertRDDEquals[T: ClassTag](expectedRDD: RDD[T], resultRDD: RDD[T])(checkEquals: (T, T) => Boolean) {
    try {
      expectedRDD.cache
      resultRDD.cache
      assert(expectedRDD.count === resultRDD.count)

      val expectedIndexValue = zipWithIndex(expectedRDD)
      val resultIndexValue = zipWithIndex(resultRDD)

      val unequalRDD = expectedIndexValue.join(resultIndexValue).
        filter { case (idx, (t1: T, t2: T)) => !checkEquals(t1, t2)
        }
      assert(unequalRDD.take(10).length === 0)
    } finally {
      expectedRDD.unpersist()
      resultRDD.unpersist()
    }
  }

  private def zipWithIndex[U: ClassTag](rdd: RDD[U]) = {
    rdd.zipWithIndex().map { case (row, idx) => (idx, row) }
  }

  override def afterAll() {
    _sc = null
    super.afterAll()
  }
}
