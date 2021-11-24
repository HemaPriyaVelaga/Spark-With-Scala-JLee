package com.sparkTutorial.sparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object StackOverFlowSurvey {

  val AGE_MIDPOINT = "age_midpoint"
  val SALARY_MIDPOINT = "salary_midpoint"
  val SALARY_MIDPOINT_BUCKET = "salary_midpoint_bucket"

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // To work with RDD, we need a Spark Context
    // To work with SparkSQL, we need a sparksession which provides a single point of entry to interact with
    // the underlying spark functionality with SQL type syntax, and allows us to deal with datasets instead of RDDs
    // Creating a Spark Session Object
    val session = SparkSession.builder().appName("StackOverFlowSurvey").master("local[1]").getOrCreate()
    // The get or create helps us to create a spark session or gets an existing one which we created earlier

    // Use the spark session to read the data which will return us a DF reader object
    val dataFrameReader = session.read

    // DF reader is a spark interface used to load a DS from external storage systems
    val responses = dataFrameReader
      .option("header", "true")
      .option("inferSchema", value = true) // Infers schema from the header and type of data
      .csv("in/2016-stack-overflow-survey-responses.csv")

    // The above returns us a data object of the type ROW

    // csv, jdbc, json, paraquet, etc can be read

    // Dataset is a distributed collection of tabular data organised into rows which make.
    // Row is a data abstraction of a ordered collection of fields
    // In our case, each row maps a line in our CSV file

    System.out.println("=== Print out schema ===")
    responses.printSchema()
    // At this point, we have a literal SQL database sit in the memory of our Spark application
    // distributed potentially on a cluster
    // We can treat the DS as a regular SQL database, which allows us to use SQL syntax to query the DS

    // SELECT country, occupation, age_midpoint, salary_midpoint FROM 2016-stack-overflow-survey-responses
    val responseWithSelectedColumns = responses.select("country", "occupation", AGE_MIDPOINT, SALARY_MIDPOINT)

    System.out.println("=== Print the selected columns of the table ===")
    responseWithSelectedColumns.show() // shows top 20 rows of the data, missing values are marked as NULL

    // The filter method can take a column object as a condition
    // The column object is a spark SQL concept which represents a col in a DS, here, it maps to original CSV file
    // col is a static method which constructs a column object from the name of the column
    System.out.println("=== Print records where the response is from Afghanistan ===")
    responseWithSelectedColumns.filter(responseWithSelectedColumns.col("country").===("Afghanistan")).show()
    // .=== is a special method which returns a column type for the equality tests

    // To find total count of each occupation
    System.out.println("=== Print the count of occupations ===")
    val groupedDataset = responseWithSelectedColumns.groupBy("occupation")  // relational dataset object os returned
    groupedDataset.count().show()  // max, min, sum, etc can be used

    System.out.println("=== Print records with average mid age less than 20 ===")
    responseWithSelectedColumns.filter(responseWithSelectedColumns.col(AGE_MIDPOINT) < 20).show()

    System.out.println("=== Print the result by salary middle point in descending order ===")
    responseWithSelectedColumns.orderBy(responseWithSelectedColumns.col(SALARY_MIDPOINT).desc).show()

    System.out.println("=== Group by country and aggregate by average salary middle point ===")
    val datasetGroupByCountry = responseWithSelectedColumns.groupBy("country")
    datasetGroupByCountry.avg(SALARY_MIDPOINT).show()

    val responseWithSalaryBucket = responses.withColumn(SALARY_MIDPOINT_BUCKET,
      responses.col(SALARY_MIDPOINT).divide(20000).cast("integer").multiply(20000))

    System.out.println("=== With salary bucket column ===")
    responseWithSalaryBucket.select(SALARY_MIDPOINT, SALARY_MIDPOINT_BUCKET).show()

    System.out.println("=== Group by salary bucket ===")
    responseWithSalaryBucket.groupBy(SALARY_MIDPOINT_BUCKET).count().orderBy(SALARY_MIDPOINT_BUCKET).show()

    session.stop()
    // Mandatory! We have to treat a sparkSQL just like a real SQL DB
  }
}
