package com.sparkTutorial.rdd.nasaApacheWebLogs

import com.sparkTutorial.rdd.nasaApacheWebLogs.UnionLogsSolution.isNotHeader
import org.apache.spark.{SparkConf, SparkContext}

object UnionLogProblem {

  def main(args: Array[String]) {

    /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
       "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
       Create a Spark program to generate a new RDD which contains the log lines from both July 1st and August 1st,
       take a 0.1 sample of those log lines and save it to "out/sample_nasa_logs.tsv" file.

       Keep in mind, that the original log files contains the following header lines.
       host	logname	time	method	url	response	bytes

       Make sure the head lines are removed in the resulting RDD.
     */

    // Solution steps:

    // Step 1: Create Spark context
    val conf = new SparkConf().setAppName("My_Union_Log_Solution").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // Step 2: Read both the files as 2 string RDDs
    val nasa1julyLines = sc.textFile("in/nasa_19950701.tsv")
    val nasa1augustLines = sc.textFile("in/nasa_19950801.tsv")


    // Step 3: Find the Union of both the RDDs
    val union = nasa1julyLines.union(nasa1augustLines)

    // Make sure to remove the header files from the union RDD
    val cleanLogLines = union.filter(line => isNotHeader(line))

    // Step 4: Take 0.1 sample out of the new RDD
    val unionSample = cleanLogLines.sample(withReplacement = true, 0.1)

    // Step 5: Now, save the final samples to a text files
    unionSample.saveAsTextFile("out/MyUnionLogProblem.text")
  }
}
