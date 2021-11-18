package com.sparkTutorial.rdd.sumOfNumbers

import com.esotericsoftware.minlog.Log.{Logger, setLogger}
import org.apache.spark.{SparkConf, SparkContext}

object SumOfNumbersProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
       print the sum of those numbers to console.

       Each row of the input file contains 10 prime numbers separated by spaces.
     */

    // Step 1: Create SparkContext
    val conf = new SparkConf().setAppName("SumOfPrimes").setMaster("local")
    val sc = new SparkContext(conf)

    // Step 2: read the data as a string RDD
    val prime_num = sc.textFile("in/prime_nums.text")

    // Step 3: Split the RDD into based on lines. Using flatmap as one line gives many outputs, filter out spaces
    val prime_num_lines = prime_num.flatMap(line => line.split("\\s+")) // delimiter is line spaces
    println("prime_num_lines: ", prime_num_lines)
    val validNum = prime_num_lines.filter(num => !num.isEmpty) // Filtering out empty strings between numbers
    println("validNum: ", validNum)
    val intNum = validNum.map(num=>num.toInt) // Converting string type num to int num
    println("intNum: ", intNum)


    // Step 4: Using reduce, perform the sum aggregation
    val sumOfPrimes = intNum.reduce((x,y) => x + y)

    println("Sum of Prime numbers: ", sumOfPrimes)

  }
}
