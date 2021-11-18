package com.sparkTutorial.rdd.airports

import com.sparkTutorial.commons.Utils
import org.apache.spark.{SparkConf, SparkContext}

object AirportsByLatitudeProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
       Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:
       "St Anthony", 51.391944
       "Tofino", 49.082222
       ...
     */

    // Step 1: Create sparkcontext
    val conf = new SparkConf().setAppName("My_Airport_Latitude_Solution").setMaster("local")
    val sc = new SparkContext(conf)

    // Step 2: Load the airports.txt file as a string RDD
    val airports = sc.textFile("in/airports.text")

    // Step 3: Split the file as lines and filter only those lines whose latitude is greater than 40
    val airportsWithGreaterLatitude = airports.filter(line => line.split(Utils.COMMA_DELIMITER) (6).toFloat > 40)
    // toFloat converts string to float

    // Step 4: Map the filtered lines to give an output having the airports name and its latitude
    val airportNameLatitude = airportsWithGreaterLatitude.map(line=>{
      // Function to return a string
      val split = line.split(Utils.COMMA_DELIMITER)
      split(1) + ", " + split(6)
    })


    // Step 5: Write the output to a text file
    airportNameLatitude.saveAsTextFile("out/airports_with_greater_latitude.txt")


  }
}
