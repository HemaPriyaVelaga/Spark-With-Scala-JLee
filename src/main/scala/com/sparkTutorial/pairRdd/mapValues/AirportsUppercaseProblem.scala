package com.sparkTutorial.pairRdd.mapValues

import com.sparkTutorial.commons.Utils
import org.apache.spark.{SparkConf, SparkContext}

object AirportsUppercaseProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text, generate a pair RDD with airport name
       being the key and country name being the value. Then convert the country name to uppercase and
       output the pair RDD to out/airports_uppercase.text

       Each row of the input file contains the following columns:

       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:

       ("Kamloops", "CANADA")
       ("Wewak Intl", "PAPUA NEW GUINEA")
       ...
     */

    // Solution steps:

    // Step 1: Create Spark Context object
    val conf = new SparkConf().setAppName("My_Aiports_Uppercase_Solution").setMaster("local")
    val sc = new SparkContext(conf)

    // Step 2: Read the file as a string RDD
    val airportStringRdd = sc.textFile("in/airports.text")

    // Step 3: Now, extract only the airport name and its country into a pair RDD
    val airportPairRDD = airportStringRdd.map(line => (line.split(Utils.COMMA_DELIMITER)(1), line.split(Utils.COMMA_DELIMITER)(3)))

    // Step 4: Use the mapVal function to convert all the country names (here, the Values in KV pair) to Uppercase
    val uppercase = airportPairRDD.mapValues(value => value.toUpperCase())

    // Step 5: Save this final pair RDD to text file
    uppercase.saveAsTextFile("out/My_Aiports_Uppercase_Solution.text")

  }
}
