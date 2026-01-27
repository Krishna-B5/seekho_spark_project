package Practise_Pgm

import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object is_holiday_5 {

  def main(args:Array[String]):Unit={

    val spark = SparkSession.builder()
      .appName("holiday")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    try{

      val data = List((1,"2024-07-27"),(2,"2024-12-25"),(3,"2025-01-01"))

      val df = spark.createDataFrame(data)
      val df1 = df.toDF("event_id","date")

      def holiday(df2:DataFrame):DataFrame ={
        df2.select(col("event_id"),col("date"),
          when(col("date").isin("2024-12-24","2025-01-01"),"True")
        .otherwise("False").alias("is_holiday"))
      }

      holiday(df1).show()
      println(" SQL Logic ")
      df1.createOrReplaceTempView("sample")
      spark.sql(
        """ select event_id,date,
          case
          when date in ("2024-12-24","2025-01-01") then "True"
          else "False"
          end as is_holiday
          from sample """).show()

    }catch {
      case e: Exception => println("Error message"+e.getMessage)
    }finally {
      spark.stop()
    }

  }
}
