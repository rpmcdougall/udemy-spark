  package com.phatlabs.ninja

  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.SparkContext

  object AmountSpentByCustomer {

    def main(args: Array[String]) {

      // Set the log level to only print errors
      Logger.getLogger("org").setLevel(Level.ERROR)

      // Create a SparkContext using every core of the local machine
      val sc = new SparkContext("local[*]", "WordCountBetter")

      // Load each line of my book into an RDD
      val input = sc.textFile("src/main/resources/SparkScala/customer-orders.csv")


      val purchases = input.map(x => x.split(",")).map( x => (x(0).toInt, x(2).toFloat))

      val spentByEach = purchases.reduceByKey( (x,y) => x+y)

      val flippedAndSorted = spentByEach.map(x => (x._2,x._1)).sortByKey()

      val results = flippedAndSorted.collect()

      for(customer <- results){
        val id = customer._2
        val total = customer._1
        println(f"Customer ID:$id, Total Spent:$total%.2f")
      }
    }

  }
