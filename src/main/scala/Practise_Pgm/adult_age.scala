package Practise_Pgm

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col,when}

object adult_age {

  def main(args:Array[String]):Unit={

//  creating a SparkSession
    val spark = SparkSession.builder()
      .appName("Adult age greater than 18")
      .master("local[*]")
      .getOrCreate()

    val emp = List(
      (1,"Ajay",28),
      (2,"Vijay",35),
      (3,"Manoj",22)
    )

    val df = spark.createDataFrame(emp)
    val df1 = df.toDF("id","name","age")

    val df2 = df1.select(
      col("id"),
      col("name"),
      col("age"),
      when(col("age") >= 18,"True")
        .otherwise("False").alias("is_adult")
    )

    df2.show()

    df1.createOrReplaceTempView("sample")

//    spark.sql(""" select * from sample""").show()

    spark.sql(
      """ select *,
        case
        when age > 18 then "True"
        else "False"
        end as status
        from sample """).show()

  }
}
