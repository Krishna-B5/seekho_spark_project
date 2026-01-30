package Practise_Pgm

import org.apache.spark.sql.functions.{col, date_format, to_date, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object payments_c9 {

  def main(args:Array[String]): Unit ={

    val spark = SparkSession.builder()
      .appName(" Quarter Payments")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    try{

      val data = List((1, "2024-05-15"), (2, "2024-01-25"), (3, "2024-11-01"), (4, "2024-07-05") )

      val df = spark.createDataFrame(data)
      val df1 = df.toDF("payment_id","payment_date")

      def payment_dat(df2: DataFrame): DataFrame ={
        df2.withColumn("quarter",when(date_format(to_date(col("payment_date"),"yyyy-MM-DD"),"MMMM").isin("January","February","March"),"Q1" )
        .when(date_format(to_date(col("payment_date"),"yyyy-MM-DD"),"MMMM").isin("April","May","June"),"Q2" )
        .when(date_format(to_date(col("payment_date"),"yyyy-MM-DD"),"MMMM").isin("July","August","September"),"Q3" )
        .otherwise("Q4")  )
      }

      payment_dat(df1).show()

      df1.groupBy(col(""))

    } catch {
      case e: Exception => println("Error Message"+e.getMessage)
    } finally {}
    spark.stop()
  }

}
