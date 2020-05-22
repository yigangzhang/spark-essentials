package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CommonTypesMySolution extends App {

  val spark = SparkSession.builder()
    .appName("Common Spark Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // adding a plain value to DF
  moviesDF.select(col("Title"), lit(47))
    .as("plain_value")
  //    .show()

  // Booleans
  val dramaFilter = col("Major_Genre") equalTo "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter

  moviesDF.select("Title")
    .where(preferredFilter)
  //    .show()
  // + multiple ways of filtering

  val moviesWithGoodnessFlagsDF = moviesDF
    .select(
      col("Title"),
      preferredFilter.as("good_movie")
    )

  moviesWithGoodnessFlagsDF
    .where("good_movie") // where(col("good_movie") === "true")
  //    .show()

  // negations
  moviesWithGoodnessFlagsDF
    .where(not(col("good_movie")))
  //    .show()

  // numbers
  val moviesAvgRatingsDF = moviesDF
    .select(
      col("Title"),
      (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2
    )
  //    .show()

  // correlation = number between -1 (negative correlated) and 1 (positive correlated), 0 means not correlated
  println(
    moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating") /* corr is an ACTION */
  )

  // Strings

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // capitalization, initcap, lower, upper
  carsDF
    .select(initcap(col("Name")))
    .show()

  // contains
  carsDF
    .select("*")
    .where(col(("Name")).contains("volkswagen"))
  //    .show()

  // regex
  val regexString = "volkswagen|vw"
  val vwDF = carsDF
    .select(
      col("Name"),
      regexp_extract(col(("Name")), regexString, 0) as "regex_extract"
    ).where(col("regex_extract") =!= "")
    .drop("regex_extract")
  //    .show()

  // regex replacing
  vwDF.select(
    col("Name"),
    regexp_replace(col("Name"), regexString, "People's Car") as "regex_replace"
  )
  //    .show()


  /**
    * Exercise
    *
    * Filter the cars DF by a list of car names obtained by an API call
    * Versions:
    *   - regexes
    *   - contains
    */

  def getCarNames: List[String] = List("Volkswagen", "Mercedes-Benz", "Ford")

  // regexes
  val complexRegex = getCarNames.map(_.toLowerCase).mkString("|") // Volkswagen|Mercedes-Benz|Ford
  carsDF
    .select(
      lower(col("Name")),
      regexp_extract(col("Name"), complexRegex, 0) as "regex_extract"
    )
    .where(col("regex_extract") =!= "")
    .drop("regex_extract")
    .show()

  // contains
  val carNameFilters = getCarNames.map(_.toLowerCase()).map(name => col("Name").contains(name))
  val bigFilter = carNameFilters.fold(lit(false))((combinedFilter, newCarNameFilter) => combinedFilter or newCarNameFilter)

  carsDF.filter(bigFilter)
    .show()
}
