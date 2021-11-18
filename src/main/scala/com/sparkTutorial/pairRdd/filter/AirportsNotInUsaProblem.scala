package com.sparkTutorial.pairRdd.filter

import com.sparkTutorial.commons.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object AirportsNotInUsaProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text;
       generate a pair RDD with airport name being the key and country name being the value.
       Then remove all the airports which are located in United States and output the pair RDD to out/airports_not_in_usa_pair_rdd.text

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located,
       IATA/FAA code, ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:

       ("Kamloops", "Canada")
       ("Wewak Intl", "Papua New Guinea")
       ...
     */

    Logger.getLogger("org").setLevel(Level.ERROR)
    // Solution steps:

    // Step 1: Create a spark Context
    val conf = new SparkConf().setAppName("AirportsNotInUSA").setMaster("local[3]")
    val sc = new SparkContext(conf)

    // Step 2: Read the file as a normal string RDD
    val allAirportDetails = sc.textFile("in/airports.text")

    // Step 3: Convert this RDD into a pair RDD with key as airport name and value as country
    val airportCountryPair = allAirportDetails.map(airportLine => (airportLine.split(Utils.COMMA_DELIMITER)(1), airportLine.split(Utils.COMMA_DELIMITER)(3)))

    // Step 4: Filter out all the airports with country name as United States
    val airportsNotInUS = airportCountryPair.filter(keyVal => keyVal._2 != "\"United States\"")

    // Step 5: Save these filtered airport tuples to a text file
    airportsNotInUS.saveAsTextFile("out/MyAirportsNotInUsaSolution.text")
  }
}
