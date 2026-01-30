package Practise_Pgm

import org.apache.spark.sql.functions.{col, datediff, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object task_duration_c4 {

  def main(args:Array[String]): Unit ={

    val spark = SparkSession.builder()
      .appName("Task Duration")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    try{

      val data = List((1, "2024-07-01", "2024-07-10"), (2, "2024-08-01", "2024-08-15"),
        (3, "2024-09-01", "2024-09-05"))

      val df = spark.createDataFrame(data)
      val df1 = df.toDF("task_id","start_date","end_date")

      def task_dura(df2: DataFrame): DataFrame ={
        df2.withColumn("task_duration",when(datediff(col("end_date"),col("start_date")) < 7,"Short")
        .when(datediff(col("end_date"),col("start_date")).between( 7 , 13),"Medium")
        .otherwise("Long")  )
      }

      task_dura(df1).show()

      df1.createOrReplaceTempView("sample")
      println("SQL Logic")
      spark.sql(
        """ select *,
          case
            when datediff(end_date, start_date) < 7 then "short"
            when datediff(end_date, start_date) between 7 and 13 then "Medium"
            else "Long"
          end as task_duration
          from sample """).show()

    } catch {
      case e: Exception => println("Error Message : "+e.getMessage)
    } finally {
      spark.stop()
    }
  }

}
