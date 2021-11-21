package com.sparkTutorial.pairRdd.aggregation.reducebykey

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/word_count.text")
    val wordRdd = lines.flatMap(line => line.split(" ")) // To words from the lines
    val wordPairRdd = wordRdd.map(word => (word, 1)) // Converts each word as a Tuple having key as the word and value as 1

    val wordCounts = wordPairRdd.reduceByKey((x, y) => x + y) // Sum the count of each tuple having the same key, here, the same word
    for ((word, count) <- wordCounts.collect()) println(word + " : " + count)
    // The collect method returns the KV pairs in the above pairRDD in the driver program as a map,
    // which can then be used to print all the KV pairs

    // Refer the reducebykey screenshot to understand what happens behind the scenes
  }
}
