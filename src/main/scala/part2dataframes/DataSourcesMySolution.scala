package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._


object DataSourcesMySolution extends App {

  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  val carsDateSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  /*
    Reading a DF:
    - format
    - schema or inferSchema = true
    - zero or more options
   */
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema)   // enforce a schema
    .option("mode", "failFast")   // dropMalformed, permissive (default)
//    .option("path", "src/main/resources/data/cars_malformed.json")
    .option("path", "src/main/resources/data/cars.json")
    .load()

  carsDF.show()

  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars_malformed.json",
    ))
    .load()

  /*
    Writing DFs
    - format
    - save mode = overwrite, append, ignore, errorIfExists
    - path
    - zero or more options
   */
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .option("path", "src/main/resources/data/output/cars_dupe.json")
    .save()
//    .save("path", "src/main/resources/data/cars_dupe.json")

  // JSON flags
  val carsDateDF = spark.read
//    .format("json")
    .option("dateFormat", "yyyy-MM-dd") // couple with schema; if Spark fails parsing, it will put null
    .option("allowSingleQuotes", "true")  // treat single quotes as double quotes
    .option("compression", "uncompressed")  // bzip2, gzip, lz4, snappy, deflate
    .schema(carsDateSchema)
//    .load("src/main/resources/data/cars_dupe.json")
    .json("src/main/resources/data/cars.json")
    .show()

  /*
    https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html
    Y = week year
    y = Year
    D = Day in year
    d = Day in month
   */

  // CSV flags
  val stockSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  spark.read
    .format("csv")
    .schema(stockSchema)
    .option("dateFormat", "MMM d yyyy") // single d here is because the data only contains single digit day, dd will parsed as null
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .load("src/main/resources/data/stocks.csv")
    .show()

  // Parquet
  carsDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/output/cars.parquet")

  // Text files
  spark.read
    .text("src/main/resources/data/sampleTextFile.txt")
    .show()

  // Reading from a remote DB
//  val employeesDF = spark.read
//    .format("jdbc")
//    .option("driver", "org.postgresql.Driver")
//    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
//    .option("user", "docker")
//    .option("password", "docker")
//    .option("dbtable", "public.employees")
//    .load()

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"
  val dbtable = "public.employees"

  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", dbtable)
    .load()

  employeesDF.show()

  /**
    * Exercise: read the movies DF, then write it as
    * - tab-separated values file
    * - snappy Parquet
    * - table "public.movies" in the Postgres DB
    */
  val moviesDF = spark.read
    .json("src/main/resources/data/movies.json")

  moviesDF.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .option("header", "true")
    .option("sep", "\t")
    .save("src/main/resources/data/output/movies.csv")

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .option("compression", "snappy")
    .save("src/main/resources/data/output/movies.parquet")

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.movies")
    .save()
}
