package com.phatlabs.ninja

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.max

/** Find the minimum temperature by weather station */
object MaxPrecipitation {

  def parseLine(line:String)= {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val precipitation = fields(3).toFloat
    (stationID, entryType, precipitation)
  }
  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MaxPrecipitation")

    // Read each line of input data
    val lines = sc.textFile("src/main/resources/SparkScala/1800.csv")

    // Convert to (stationID, entryType, temperature) tuples
    val parsedLines = lines.map(parseLine)

    // Filter out all but TMIN entries
    //    modified to MAX for example exercise
    val maxPrecip = parsedLines.filter(x => x._2 == "PRCP")

    // Convert to (stationID, temperature)
    val stationTemps = maxPrecip.map(x => (x._1, x._3.toFloat))

    // Reduce by stationID retaining the minimum temperature found
    val minTempsByStation = stationTemps.reduceByKey( (x,y) => max(x,y))

    // Collect, format, and print the results
    val results = minTempsByStation.collect()

    for (result <- results.sorted) {
      val station = result._1
      val precip = result._2
      val formattedPrecip = f"$precip%.2f"
      println(s"$station max precip: $formattedPrecip")
    }

  }
}