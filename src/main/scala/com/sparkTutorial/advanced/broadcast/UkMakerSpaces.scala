package com.sparkTutorial.advanced.broadcast

import com.sparkTutorial.commons.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

// Question: How are the makerspaces distributed across different regions in UK?
// We have 2 datasets. One is the in/uk-makerspaces-identifiable-data.csv which has the information about  email address, postcode, etc
// To find the distribution, we need to know which postcode belongs to which region
// This information is present in the in/uk-postcode.csv file
object UkMakerSpaces {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("UkMakerSpaces").setMaster("local[1]")
    val sparkContext = new SparkContext(conf)

    // Load the postcode dataset and broadcast it across the cluster
    val postCodeMap = sparkContext.broadcast(loadPostCodeMap())

    val makerSpaceRdd = sparkContext.textFile("in/uk-makerspaces-identifiable-data.csv")

    // Look up the region using the postcode of the makerspace
    val regions = makerSpaceRdd
      // filter header line
      .filter(line => line.split(Utils.COMMA_DELIMITER, -1)(0) != "Timestamp")
      // matching with the postcode prefix
      .filter(line => getPostPrefix(line).isDefined)
      // if not post prefix, return unknown
      .map(line => postCodeMap.value.getOrElse(getPostPrefix(line).get, "Unknown"))

    // Count the makerspaces in same regions
    for ((region, count) <- regions.countByValue()) println(region + " : " + count)
  }

  def getPostPrefix(line: String): Option[String] = {
    val splits = line.split(Utils.COMMA_DELIMITER, -1)
    val postcode = splits(4)
    if (postcode.isEmpty) None else Some(postcode.split(" ")(0))
  }

  def loadPostCodeMap(): Map[String, String] = {
    Source.fromFile("in/uk-postcode.csv").getLines.map(line => {
      val splits = line.split(Utils.COMMA_DELIMITER, -1)
      splits(0) -> splits(7) // Key (postcode prefix), Value (region name)
    }).toMap
  }
}
