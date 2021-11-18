package com.sparkTutorial.rdd.nasaApacheWebLogs

import com.sparkTutorial.rdd.nasaApacheWebLogs.UnionLogsSolution.isNotHeader
import org.apache.spark.{SparkConf, SparkContext}

object SameHostsProblem {

  def main(args: Array[String]) {

    /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
       "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
       Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
       Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

       Example output:
       vagrant.vf.mmc.com
       www-a1.proxy.aol.com
       .....

       Keep in mind, that the original log files contains the following header lines.
       host	logname	time	method	url	response	bytes

       Make sure the head lines are removed in the resulting RDD.
     */

    // Step 1: Create Spark context
    val conf = new SparkConf().setAppName("My_SAME_Hosts_Solution").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // Step 2: Read both the files as 2 string RDDs
    val nasa1julyLines = sc.textFile("in/nasa_19950701.tsv")
    val nasa1augustLines = sc.textFile("in/nasa_19950801.tsv")


    // Step 3: Find the Union/Intersection of both the RDDs
    //val union = nasa1julyLines.union(nasa1augustLines)
    // The problem with union is that it may have duplicate elements if they are duplicated in both the RDDs
    val intersectingHosts = nasa1julyLines.union(nasa1augustLines)
    val distinctHosts = intersectingHosts.distinct()
      // we can eliminate these two steps if we take a look at the alternative solution provided by the author


    // Make sure to remove the header files from the union RDD
    val cleanLogLines = distinctHosts.filter(line => isNotHeader(line))

    // Step 4: Now, we need to get only the hostnames used on both the days
    val hostnames = cleanLogLines.map(line => {
      val split = line.split("\t")
      split(0)
    })

    hostnames.saveAsTextFile("out/MySameHostsSolution.txt")


    // Another better way to solve this is to first filter out only hostnames and then apply union operation . This saves a lot of data
  }
}
