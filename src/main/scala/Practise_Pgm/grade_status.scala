package Practise_Pgm

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col,when}

object grade_status {

  def main(args:Array[String]):Unit={

    val spark = SparkSession.builder()
      .appName(" Grade pgm")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val grades = List(
      (1,85),
      (2,42),
      (3,73)
    ).toDF("student_id","score")

    val result = grades.select(
      col("student_id"),
      col("score"),
      when(col("score") >= 50,"Pass")
        .otherwise("Fail").alias("Status")
    )

    result.show()

    grades.createOrReplaceTempView("student")

    spark.sql(
      """ select *,
        case
        when score >= 50 then "Pass"
        else "Fail"
        end as status
        from student
        """).show()
  }

}
