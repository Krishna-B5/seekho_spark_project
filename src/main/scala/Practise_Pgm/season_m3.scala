package Practise_Pgm

import org.apache.spark.sql.functions.{col, date_format, to_date, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object season_m3 {

  def main(args:Array[String]):Unit ={

    val spark = SparkSession.builder()
      .appName("Finding the seasons")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    try{

      val data = List((1,"2025-07-01"),(2,"2025-12-01"), (3,"2025-05-01"))

      val df = spark.createDataFrame(data)
      val df1 = df.toDF("order_id","order_date")

      def seasons(df2: DataFrame): DataFrame ={
        df2.select(col("order_id"),col("order_date"),
          when(date_format(to_date(col("order_date"),"yyyy-MM-dd"), "MMMM").isin("June","July","August"),"Summer")
            .when(date_format(to_date(col("order_date"),"yyyy-MM-dd"), "MMMM").isin("December","January","February"),"Winter")
            .otherwise("Other").alias("Seasons")
        )
      }

      seasons(df1).show()

      df1.createOrReplaceTempView("sample")
      println("SQL Logic")
      spark.sql(
        """ select *,
          case
            WHEN month(order_date) IN (6, 7,8) THEN 'Summer'
            WHEN month(order_date) IN (12,1,2) THEN 'Winter'
            ELSE 'other'
          end as seasons
          from sample
          """).show()

    } catch {
      case e : Exception => println("Error Message :"+e.getMessage)
    } finally {
      spark.stop()
    }
  }

}
