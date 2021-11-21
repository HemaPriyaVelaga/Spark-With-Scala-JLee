package com.sparkTutorial.pairRdd.aggregation.reducebykey.housePrice

import com.sparkTutorial.commons.Utils
import org.apache.spark.{SparkConf, SparkContext}

object AverageHousePriceProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the house data from in/RealEstate.csv,
       output the average price for houses with different number of bedrooms.

    The houses dataset contains a collection of recent real estate listings in San Luis Obispo county and
    around it. 

    The dataset contains the following fields:
    1. MLS: Multiple listing service number for the house (unique ID).
    2. Location: city/town where the house is located. Most locations are in San Luis Obispo county and
    northern Santa Barbara county (Santa Maria Orcutt, Lompoc, Guadelupe, Los Alamos), but there
    some out of area locations as well.
    3. Price: the most recent listing price of the house (in dollars).
    4. Bedrooms: number of bedrooms.
    5. Bathrooms: number of bathrooms.
    6. Size: size of the house in square feet.
    7. Price/SQ.ft: price of the house per square foot.
    8. Status: type of sale. Thee types are represented in the dataset: Short Sale, Foreclosure and Regular.

    Each field is comma separated.

    Sample output:

       (3, 325000)
       (1, 266356)
       (2, 325000)
       ...

       3, 1 and 2 mean the number of bedrooms. 325000 means the average price of houses with 3 bedrooms is 325000.
     */


    // Solution Steps:

    // Step 1: Create Spark Context
    val conf = new SparkConf().setAppName("Avg_House_Price").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Step 2: Read the text file as a string RDD
    val housePriceLines = sc.textFile("in/RealEstate.csv")
    // Removing headers
    val cleanedLines = housePriceLines.filter(line => !line.contains("Bedrooms"))


    // Step 3: Extract Bedrooms and Price as Key and Value with the help of map into a pair RDD
    val bedroomPricePairRDD = cleanedLines.map(line => (line.split(Utils.COMMA_DELIMITER)(3),(1, line.split(Utils.COMMA_DELIMITER)(2).toDouble)))


    // Step 4: Count the sum total of price for all houses having same number of bedrooms and the count of such houses
    val numSimilarBedrooms = bedroomPricePairRDD.reduceByKey((valueOfKey1, valueOfKey2) => ((valueOfKey1._1 + valueOfKey2._1),(valueOfKey1._2 + valueOfKey2._2)))

    // Step 5: Calculate the average price
    val avgPriceByBedrooms = numSimilarBedrooms.mapValues(value => value._2/value._1)

    // Step 6: Print the tupl (numBedrooms, avg price)
    for ((bedroom, avg) <- avgPriceByBedrooms.collect()) println(bedroom + " : " + avg)
  }

}
