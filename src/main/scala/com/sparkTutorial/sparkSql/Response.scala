package com.sparkTutorial.sparkSql

// We can create our own custom case class having all the attribute names and types same as that of the original dataset
// So that Spark can map the input dataframe to a strongly typed spark dataset of the type of our class object
case class Response(country: String, age_midpoint: Option[Double], occupation: String, salary_midpoint: Option[Double])
