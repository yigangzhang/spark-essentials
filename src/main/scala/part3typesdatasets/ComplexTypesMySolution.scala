package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object ComplexTypesMySolution extends App {

  val spark = SparkSession.builder()
    .appName("Complex Data Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // Dates
  val moviesWithReleaseDates = moviesDF
    .select(
      col("Title"),
      to_date(
        col(("Release_Date")),
        "dd-MMM-yy"
      ) as "Actual_Release"
    )
    .withColumn("Today", current_date())  // today
    .withColumn("Right_Now", current_timestamp()) // this second
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365)

  moviesWithReleaseDates.select("*").where(col("Actual_Release").isNull)
//    .show()

  /**
    * Exercise
    * 1. How do we deal with multiple date formats?
    * 2. Read the stocks DF and parse the dates
    */

  // 1 - parse the DF multiple times, then union the small DFs
  val moviesWithReleaseDates2 = moviesDF
    .select(
      col("Title"),
      when(
        to_date(col(("Release_Date")), "dd-MMM-yy").isNotNull,
        to_date(col(("Release_Date")), "dd-MMM-yy"),
      ).when(
          to_date(col(("Release_Date")), "d-MMM-yy").isNotNull,
          to_date(col(("Release_Date")), "d-MMM-yy"),
      ).when(
        to_date(col(("Release_Date")), "yyyy-MM-dd").isNotNull,
        to_date(col(("Release_Date")), "yyyy-MM-dd"),
      ).otherwise("Unknown Format")
      as "Actual_Release"
    )
//    .show()


  // 2
  val stocksDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/data/stocks.csv")

  stocksDF
    .withColumn(
      "actual_date",
      to_date(
        col("date"),
        "MMM d yyyy"
      )
    )
//    .show()

  // Structures

  // 1 - with col operators
  moviesDF.select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"))
    .select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit"))
//    .show()

  // 2 - with expression strings
  moviesDF.selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross as US_Profit")
    .show()

  // Arrays

  val moviesWithWords = moviesDF.select(
    col("Title"),
    split(col("Title"), " |,")
      .as(("Title_Words"))
  ) // ARRAY of Strings

  moviesWithWords.select(
    col("Title"),
    expr("Title_Words[0]"),
    size(col("Title_Words")),
    array_contains(col("Title_Words"), "Love")
  )
}

