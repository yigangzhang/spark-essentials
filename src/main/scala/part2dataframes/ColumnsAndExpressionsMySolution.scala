package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressionsMySolution extends App {

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  carsDF.show()

  // Columns
  val firstColumn = carsDF.col("Name")

  // Selecting (projecting)
  val carNamesDF = carsDF.select(firstColumn)

  carNamesDF.show()

  // various select methods
  import spark.implicits._
  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year,
    $"Horsepower",
    expr("Origin")
  ).show()

  // select with plain column names
  carsDF.select("Name", "Year")
    .show()

  // Expressions
  val simpleExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression as "Weight_in_kg",
    expr("Weight_in_lbs / 2.2") as "Weight_in_kg2"
  )

  carsWithWeightsDF.show()

  // selectExpr
  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  // DF processing

  // adding a column
  val carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)
  // renaming a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  // careful with columns names
  carsWithColumnRenamed.selectExpr("`Weight in pounds`")
  // remove a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  // filtering
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA")
  // filtering with expression strings
  val americanCarsDF = carsDF.filter(col("Origin") === "USA")
  val americanCarsDF2 = carsDF.filter("Origin = 'USA'")
  // chain filters
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerfulCarsDF2 = carsDF.filter((col("Origin") === "USA").and(col("Horsepower") > 150))
  val americanPowerfulCarsDF3 = carsDF.filter("Origin ='USA' and Horsepower > 150")

  // unioning = adding more rows
   val moreCarsDF = spark.read
     .option("inferSchema", "true")
     .json("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF) // works if the DFs have the same schema

  // distinct values
  val allCountriesDF = carsDF.select("Origin").distinct()
  allCountriesDF.show()

  /**
    * Exercises
    *
    * 1. Read the movies DF and select 2 columns of your choice
    * 2. Create another column summing up the total profit of the movies = US_Gross + Worldwide_Gross + DVD sales
    * 3. Select all COMEDY movies with IMDB rating above 6
    *
    * Use as many versions as possible
    */

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")
  moviesDF.show()

  val moviesIMDBRatingDF = moviesDF.select("Title", "IMDB_Rating")
  val moviesIMDBRatingDF2 = moviesDF.select(col("Title"), col("IMDB_Rating"))
  val moviesIMDBRatingDF3 = moviesDF.selectExpr("Title", "IMDB_Rating")
  val moviesIMDBRatingDF4 = moviesDF.select(moviesDF.col("Title"), col("IMDB_Rating"))
  val moviesIMDBRatingDF5 = moviesDF.select($"Title", expr("IMDB_Rating"))

  val moviesProfitDF = moviesDF.withColumn("total_profit", col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales"))
  val moviesProfitDF2 = moviesDF.withColumn("total_profit", expr("US_Gross + Worldwide_Gross + US_DVD_Sales"))

  val moviesProfitDF3 = moviesDF.select(
    col("Title"),
    col("US_Gross"),
    col("Worldwide_Gross"),
    col("US_DVD_Sales"),
    (col("US_Gross") + col("Worldwide_Gross")) as("Total_Gross"))
  moviesProfitDF3.show()

  val moviesProfitDF4 = moviesDF.selectExpr(
    "Title",
    "US_Gross",
    "Worldwide_Gross",
    "US_Gross + Worldwide_Gross as Total_Gross")
  moviesProfitDF4.show()

  val moviesProfitDF5 = moviesDF.select(
    "Title",
    "US_Gross",
    "Worldwide_Gross")
    .withColumn("Total_Gross",col("US_Gross") + col("Worldwide_Gross"))
  moviesProfitDF5.show()

  val okComediesDF = moviesDF
    .select("Title", "Major_Genre","IMDB_Rating")
    .where("Major_Genre = 'Comedy' and IMDB_Rating > 6")
  okComediesDF.show()

  val okComediesDF2 = moviesDF
    .select("Title", "Major_Genre","IMDB_Rating")
    .filter("Major_Genre = 'Comedy' and IMDB_Rating > 6")
  okComediesDF2.show()

  val okComediesDF3 = moviesDF
    .select("Title", "Major_Genre","IMDB_Rating")
    .where("Major_Genre = 'Comedy'")
    .where("IMDB_Rating > 6")
  okComediesDF3.show()

  val okComediesDF4 = moviesDF
    .select(col("Title"), col("Major_Genre"), col("IMDB_Rating"))
    .where(col("Major_Genre") === "Comedy")
    .where(col("IMDB_Rating") > 6)
  okComediesDF4.show()

  val okComediesDF5 = moviesDF
    .select(col("Title"), col("Major_Genre"), col("IMDB_Rating"))
    .where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)
  okComediesDF5.show()

//  moviesDF.filter(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6).show()
//  moviesDF.where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6).show()
}

