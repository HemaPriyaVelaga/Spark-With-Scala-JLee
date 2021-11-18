package com.sparkTutorial.rdd.persist

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

object PersistExample {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("reduce").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val inputIntegers = List(1, 2, 3, 4, 5)
    val integerRdd = sc.parallelize(inputIntegers)

    integerRdd.persist(StorageLevel.MEMORY_ONLY)  // Persisting the RDD in the memory storage level
    // Other available levels are disk, and a combination of disk and memory and can be also stored as serialised objects

    integerRdd.reduce((x, y) => x * y) // The first action called on RDD -> Here, Spark parallelizes the RDD and persists it and then perfoms reduce action
    integerRdd.count() // Another action is called on the same RDD. In this case, since this RDD is already persisted, it doesnt call
    // the parallelize method again and performs the count operation on the already persisted RDD
  }
}
