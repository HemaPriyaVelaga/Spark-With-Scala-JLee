package com.sparkTutorial.pairRdd.groupbykey

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object GroupByKeyVsReduceByKey {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("GroupByKeyVsReduceByKey").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val words = List("one", "two", "two", "three", "three", "three")
    val wordsPairRdd = sc.parallelize(words).map(word => (word, 1))

    // Using Reduce By Key, it just sums up all the values for a common key, here, the value is 1 everytime so the final count will be
    // equal to the number of times the word has occurred in the list
    val wordCountsWithReduceByKey = wordsPairRdd.reduceByKey((x, y) => x + y).collect()
    println("wordCountsWithReduceByKey: " + wordCountsWithReduceByKey.toList)

    // If we use Group By, it will return a pair RDD with key as the word and values as an iterable having a number of 1s. We have to
    // then calculate the size of the iterable to find the number of repetitions of that word
    val wordCountsWithGroupByKey = wordsPairRdd.groupByKey().mapValues(intIterable => intIterable.size).collect()
    println("wordCountsWithGroupByKey: " + wordCountsWithGroupByKey.toList)
  }
}

