package com.sparkTutorial.rdd

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark._

object WordCount {

  def main(args: Array[String]) {

    // Setting the logging level to error in order to see only important messages on our console
    Logger.getLogger("org").setLevel(Level.ERROR)

    // The next two lines are the starting point of spark core
    // Spark Conf object specifies various parameters for a spark application
    // "local" = runs on 1 core (1 thread/1 worker)
    // "local[*]" = runs on all cores (Multiple threads/workers)
    // local[num] = runs on 'num' number of cores (num threads/workers)
    val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]") // Uses upto 3 cores on our local machine to run the spark job
    val sc = new SparkContext(conf) // It is the main entry point for spark functionality
    // Spark context represents connection to a spark cluster and can be used to create RDDs, accumulators, etc on that cluster

    val lines = sc.textFile("in/word_count.text") // Loading the wordcount article as a string Resilient Distributed Dataset (RDD)
    val words = lines.flatMap(line => line.split(" ")) // Splitting the article into separate words based on the space character
    // Here, we used flatmap instead of map because, 1 input line gives n output strings (1:many relation between input and output RDD)

    val wordCounts = words.countByValue() // Count the occurrence of each word
    // Here, we used countByValue instead of count because, if we use count, it wil just return the total number of words present in the RDD
    for ((word, count) <- wordCounts) println(word + " : " + count) // Printing out the results
  }
}
