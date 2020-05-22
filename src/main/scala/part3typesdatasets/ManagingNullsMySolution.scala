package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ManagingNullsMySolution extends App {
  val spark = SparkSession.builder()
    .appName("Managing Nulls")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // select the first non-null value
  moviesDF.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10)
  )
//    .show()

  // checking for nulls
  moviesDF.select("*").where(col("Rotten_Tomatoes_Rating").isNull)

  // nulls when ordering
  moviesDF.orderBy(col("IMDB_Rating").desc_nulls_last)

  // removing nulls
  moviesDF.select("Title", "IMDB_Rating").na.drop() // remove rows containing nulls
//      .where(col("IMDB_Rating").isNull)
//      .show()

  // replace nulls
  moviesDF.na.fill(0, List("IMDB_Rating", "Rotten_Tomatoes_Rating"))
//    .show()

  moviesDF.na.fill(Map(
    "IMDB_Rating" -> 0,
    "Rotten_Tomatoes_Rating" -> 10,
    "Director" -> "Unknown"
  ))
//    .show()

  // complex operations
  moviesDF
    .selectExpr(
    "Title",
    "Rotten_Tomatoes_Rating",
    "IMDB_Rating",
    "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as ifnull",  // same as coalesce
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nvl",   // same
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10) nullif",  // returns null if the two values are EQUAL, else first value
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0) nvl2"  // if (first != null) second else third
  )
//    .where(col("Rotten_Tomatoes_Rating").isNotNull and col("IMDB_Rating").isNotNull)
//    .where(col("Rotten_Tomatoes_Rating") === col("IMDB_Rating") * 10)
    .show()

}
