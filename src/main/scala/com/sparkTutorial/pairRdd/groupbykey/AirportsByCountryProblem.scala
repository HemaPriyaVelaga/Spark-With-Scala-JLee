package com.sparkTutorial.pairRdd.groupbykey

import com.sparkTutorial.commons.Utils
import org.apache.spark.{SparkConf, SparkContext}

object AirportsByCountryProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text,
       output the the list of the names of the airports located in each country.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:

       "Canada", List("Bagotville", "Montreal", "Coronation", ...)
       "Norway" : List("Vigra", "Andenes", "Alta", "Bomoen", "Bronnoy",..)
       "Papua New Guinea",  List("Goroka", "Madang", ...)
       ...
     */

    // Solution Steps:

    // Step 1: Create Spark Context Object
    val conf = new SparkConf().setAppName("AirportsByCountry").setMaster("local[3]")
    val sc = new SparkContext(conf)

    // Step 2: Read the file as a String RDD
    val airportsData = sc.textFile("in/airports.text")

    // Step 3: Now, create a pair RDD with country as the key and the airport as the value
    val airportCountryPairRDD = airportsData.map(airportData => (airportData.split(Utils.COMMA_DELIMITER)(3),airportData.split(Utils.COMMA_DELIMITER)(1)))

    // Step 4: Now, groupBy the key which will return a pair RDD with key as country and value as the pointer to a list containing
    // all the airports from that coutry
    val airportGroupByCountry = airportCountryPairRDD.groupByKey();

    // Step 5: Now, print out the key value pairs with the help of collect as map functionaity in Spark
    for((country, airport) <- airportGroupByCountry.collectAsMap())
      println(country+ ": " + airport.toList)

  }
}
