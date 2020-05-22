package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AggregationsMySolution extends App {
  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  moviesDF.show()

  // counting
  val genresCountDF = moviesDF.select(count(col("Major_Genre"))) // all the values except null
  genresCountDF.show()
  moviesDF.selectExpr("count(Major_Genre)")

  // counting all
  moviesDF.select(count("*")).show() // count all the rows, and will INCLUDE nulls

  // counting distinct
  moviesDF.select(countDistinct("Major_Genre")).show()

  // approximate count
  moviesDF.select(approx_count_distinct(col("Major_Genre"))).show()

  // min and max
  val minRatingDF = moviesDF.select(min(col("IMDB_Rating")))
  minRatingDF.show()
  moviesDF.selectExpr("min(IMDB_Rating)").show()

  // sum
  moviesDF.select(sum("US_Gross")).show()
  moviesDF.selectExpr("sum(US_Gross)").show()

  // ave
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")

  // data science
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  ).show()

  // Grouping
  val countByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .count() // select count(*) from moviesDF group by Major_Genre
  countByGenreDF.show()

  val avgRatingByGenreDF = moviesDF
    .groupBy("Major_Genre")
    .avg("IMDB_Rating")
  avgRatingByGenreDF.show()

  val aggregationRatingByGenreDF = moviesDF
    .groupBy("Major_Genre")
    .agg(
      count("*") as "movies",
      avg("IMDB_Rating") as "Avg_Rating"
    )
    .orderBy("Avg_Rating")
  aggregationRatingByGenreDF.show()

  /**
    * Exercises
    *
    * 1. Sum up ALL the profits of ALL the movies in the DF
    * 2. Count how many distinct directors we have
    * 3. Show the mean and standard deviation of US gross revenue for the movies
    * 4. Compute the average IMDB rating and the average US gross revenue PER DIRECTOR
    */

  // 1
  moviesDF
    .selectExpr(
      "US_Gross + Worldwide_Gross + US_DVD_Sales as Total_Gross")
    .select(sum("Total_Gross"))
    .show()

  // 2
  moviesDF
    .select(countDistinct("Director"))
    .show()

  // Why it's off by 1?
  val countDir = moviesDF
    .select("Director")
    .distinct()
    .count()
  println(countDir)

  // 3
  moviesDF
    .select(
      mean("US_Gross"),
      stddev("US_Gross")
    )
    .show()


  // 4
  moviesDF
    .groupBy("Director")
    .agg(
      avg("IMDB_Rating") as "Avg_Rating",
      avg("US_Gross")
    )
    .orderBy(col("Avg_Rating").desc_nulls_last)
    .show()

//  {
//    "Title": "The Land Girls"
//    ,
//    "US_Gross": 146083
//    , "Worldwide_Gross": 146083
//    , "US_DVD_Sales": null
//    , "Production_Budget": 8000000
//    , "Release_Date": "12-Jun-98"
//    , "MPAA_Rating": "R"
//    , "Running_Time_min": null
//    , "Distributor": "Gramercy"
//    , "Source": null
//    , "Major_Genre": null
//    , "Creative_Type": null
//    , "Director": null
//    , "Rotten_Tomatoes_Rating": null
//    , "IMDB_Rating": 6.1
//    , "IMDB_Votes": 1071
//  }
}
