package part3typesdatasets

import java.sql.Date

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}


object DatasetsMySolution extends App {

  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()

  // convert a DF to a DataSet
  implicit val intEncoder = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int]

  // dataset of a complex type
  // 1 - define your case class
  case class Car(
                  Name: String,
                  Miles_per_Gallon: Option[Double],
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Option[Long],
                  Weight_in_lbs: Long,
                  Acceleration: Double,
//                  Year: Date,
                  Year: String,
                  Origin: String
                )

  // 2 - read the DF from the file
  def readJson(filename: String) = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$filename.json")

  // 3 - define an encoder (importing the implicits)
  import spark.implicits._
  val carsDF = readJson("cars")
  //  implicit val carEncoder = Encoders.product[Car]

  // 4 - convert the DF to DS
  val carsDS = carsDF.as[Car]

  // DS collection functions
  numbersDS.filter(_ < 100).show()

  // map, flatMap, fold, reduce, for comprehension ...
  val carNameDS = carsDS.map(car => car.Name.toUpperCase())
  carNameDS.show()

  /**
    * Exercises
    *
    * 1. Count how many cars we have
    * 2. Count how many POWERFUL cars we have (HP > 140)
    * 3. Average HP for the entire dataset
    */

  // 1
  val carsCount = carsDS.count
  println(carsCount)

  // 2
  println(carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count)

  // 3
  carsDS.agg(avg("Horsepower")).show()
  carsDS.select(avg("Horsepower")).show()
  val avgHP = carsDS.map(_.Horsepower.getOrElse(0L)).reduce((_+ _)) / carsCount
  println(avgHP)

  // Joins
  case class Guitar(id: Long, make: String, model: String, guitarType: String)
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarDS = readJson("guitars").as[Guitar]
  val guitarPlayersDS = readJson("guitarPlayers").as[GuitarPlayer]
  val bandDS = readJson("bands").as[Band]

  val guitarPlayerBandDS: Dataset[(GuitarPlayer, Band)] = guitarPlayersDS
    .joinWith(bandDS,
      guitarPlayersDS.col("band") === bandDS.col("id"),
      "inner")
  guitarPlayerBandDS.show()

  /**
    * Exercise: join the guitarsDS and guitarPlayersDS, in an outer join
    * (hint: use array_contains)
    */

  guitarPlayersDS
    .joinWith(
      guitarDS,
      array_contains(
        guitarPlayersDS.col("guitars"),
        guitarPlayersDS.col("id")
      ),
      "outer"
    )
    .show()

  // Grouping DS
  val carsGroupByOrigin = carsDS
    .groupByKey(_.Origin)
    .count()
    .show()

  // joins and groups are WIDE transformations, will involve SHUFFLE operations
}

