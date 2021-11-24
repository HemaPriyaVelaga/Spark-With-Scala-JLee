package com.sparkTutorial.pairRdd.sort

import org.apache.spark.{SparkConf, SparkContext}


object SortedWordCountProblem extends App {

    /* Create a Spark program to read the an article from in/word_count.text,
       output the number of occurrence of each word in descending order.

       Sample output:

       apple : 200
       shoes : 193
       bag : 176
       ...
     */

  // Solution Steps:

  // Step 1: Create a spark context Object
  val conf = new SparkConf().setAppName("SortedWordCount").setMaster("local")
  val sc = new SparkContext(conf)

  // Step 2: Read the file as a string RDD
  val lines = sc.textFile("in/word_count.text")
  val words = lines.flatMap(line => line.split(" "))

  // Step 3: Now, create a pair RDD with word as the key and value as 1
  val wordPairRDD = words.map(word => (word, 1))

  // Step 4: Now, Reduce By Key to find the count of the word
  val wordCount = wordPairRDD.reduceByKey((x, y) => x + y);

  // Step 5: Reverse the pair such that the value is the key and key is the value
  val reversedPair = wordCount.map(word => (word._2, word._1))

  // Step 6: Sort by the number of occurrences of the word in descending order
  val sortedWordCount = reversedPair.sortByKey(ascending = false)

  // Step 7: Collect the tuple and print it
  for((count, word) <- sortedWordCount.collect())
    println(word + ": " + count)

}

