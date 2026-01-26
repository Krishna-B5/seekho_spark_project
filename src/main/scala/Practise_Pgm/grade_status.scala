package Practise_Pgm

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, when}

object grade_status {

  def main(args:Array[String]):Unit={

    val spark = SparkSession.builder()
      .appName(" Grade pgm")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

try {
  val grades = List((1, 85), (2, 42), (3, 73)
  ).toDF("student_id", "score")

  def grades_status(df: DataFrame): DataFrame = {
    df.select(col("student_id"), col("score"),
      when(col("score") >= 50, "Pass")
        .otherwise("Fail").alias("Status") )
  }

  grades_status(grades).show()

  grades.createOrReplaceTempView("student")

  spark.sql(
    """ select *,
        case when score >= 50 then "Pass"
        else "Fail"
        end as status
        from student
        """).show()
} catch {
  case e: Exception => println(" Error Message :" +e.getMessage)
} finally {
  spark.stop()
}
  }
}
