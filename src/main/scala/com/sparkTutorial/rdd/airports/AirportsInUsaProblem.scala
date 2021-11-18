package com.sparkTutorial.rdd.airports

import com.sparkTutorial.commons.Utils
import com.sparkTutorial.commons.Utils.COMMA_DELIMITER
import org.apache.spark.{SparkConf, SparkContext}

object AirportsInUsaProblem {
  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
       and output the airport's name and the city's name to out/airports_in_usa.text.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:
       "Putnam County Airport", "Greencastle"
       "Dowagiac Municipal Airport", "Dowagiac"
       ...
     */


    // Steps to solve the problem:

    // Step 1: Create a Spark Context object by passing it the sparkConf object

    val conf = new SparkConf().setAppName("My_Airports_Solution").setMaster("local[*]")
    val sc = new SparkContext(conf)


    // Step 2: Load the airports.text file from the 'in' folder in this project as a string RDD
    val airportLines = sc.textFile("in/airports.text")

    // Step 3: Split it based on lines and filter only those lines which are located in United States
    val airportsInUS = airportLines.filter(line => line.split(Utils.COMMA_DELIMITER) (3) ==  "\"United States\"")
    // Here, first each line is split based on commas into an ordered group of strings. Now, the countryname is index 3 (4th position) in that group.
    // So, we filter only those lines based on United states country

    // Step 4: Map the filtered airport line to their corresponding cities
    val airportsWithCities = airportsInUS.map(line =>
      {
        // This is a function (in scala) which takes an input line, splits it based on comma and returns only 2 elements as string from that split line
        val split = line.split(Utils.COMMA_DELIMITER)
        split(1) + ", " + split(2)  //name of airport, city of airport
      }
    )
    // Here, we use map instead of flatmap because, for each input line, we give a single output string. 1:1 relation between input and output RDD rows

    // Step 5: Finally, save this output to a text file in the output folder
    airportsWithCities.saveAsTextFile("out/US_airports_with_cities.text")
    // The number of output files will be equal to the number of cores(workers) we mentioned in our application to be used

  }
}
