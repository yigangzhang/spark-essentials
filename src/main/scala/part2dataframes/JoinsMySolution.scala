package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object JoinsMySolution extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")


  val guitaristDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  // joins
  val joinCondition = guitaristDF.col("band") === bandDF.col("id")
  val guitaristsBandsDF = guitaristDF
    .join(
      bandDF,
      joinCondition
      )

  guitaristsBandsDF.show()

  // outer joins
  // left outer = everything in the inner join + all the rows in the LEFT table, with nulls in where the data is missing
  guitaristDF.join(bandDF, joinCondition, "left_outer")

  // right outer = everything in the inner join + all the rows in the RIGHT table, with nulls in where the data is missing
  guitaristDF.join(bandDF, joinCondition, "right_outer")

  // full outer = everything in the inner join + all the rows in the BOTH table, with nulls in where the data is missing
  guitaristDF.join(bandDF, joinCondition, "full_outer")

  // semi-joins = inner join with columns only from the LEFT
  // semi-joins = everything in the left DF for which there is a row in the right DF satisfying the condition
  guitaristDF.join(bandDF, joinCondition, "left_semi").show()

  // anti-joins = inner join with columns only from the LEFT for missing rows only
  // anti-joins = everything in the left DF for which there is NO row in the right DF satisfying the condition
  guitaristDF.join(bandDF, joinCondition, "left_anti").show()

  // thing to bear in mind
  // sql.AnalysisException: Reference 'id' is ambiguous, could be: id, id.;
//  guitaristsBandsDF.select("id", "band").show() // this will crash

  // option 1 - rename the column on which we are joining
//  guitaristDF.join(bandDF, "id").show()
  guitaristDF.join(bandDF.withColumnRenamed("id", "band"), "band").show()

  // option 2 - drop the dupe column
  guitaristsBandsDF.drop(bandDF.col("id")).show()

  // option 3 - rename the offending column and keep the data
  val bandsModDF = bandDF.withColumnRenamed("id", "bandId")
  guitaristDF.join(bandsModDF, guitaristDF.col("band") === bandsModDF.col("bandId")).show()

  // using complex types
  guitaristDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)")).show()

  /**
    * Exercises
    *
    * 1. show all employees and their max salary
    * 2. show all employees who were never managers
    * 3. find the job titles of the best paid 10 employees in the company
    */

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load()

  val employeesDF = readTable("employees")
  val deptManagerDF = readTable("dept_manager")
  val salariesDF = readTable("salaries")
  val titlesDF = readTable("titles")

  // 1
  salariesDF
    .groupBy("emp_no")
    .max("salary")
    .join(employeesDF, "emp_no")
    .show()

  // 2
  employeesDF
    .join(
      deptManagerDF,
      employeesDF.col("emp_no") === deptManagerDF.col("emp_no"),
      "left_anti")
    .show()

  // 3
// val mostRecentJobTitlesDF = titlesDF.groupBy("emp_no")

  salariesDF
    .groupBy("emp_no")
    .max("salary")
    .orderBy(col("max(salary)").desc)
    .limit(10)
    .join(titlesDF, salariesDF.col("emp_no") === titlesDF.col("emp_no"))
    .drop(salariesDF.col("emp_no"))
    .groupBy("emp_no", "title")
    .agg(
      max("to_date")
    )
    .join(employeesDF, "emp_no")
    .show()
}
