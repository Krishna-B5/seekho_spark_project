package Practise_Pgm

import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object is_morning_m5 {

  def main(args:Array[String]):Unit ={

    val spark = SparkSession.builder()
      .appName("Morning or not")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    try{

      val data = List((1,"09:00"),(2,"18.30"),(3,"14:00"))

      val df = spark.createDataFrame(data)
      val df1 = df.toDF("login_id","login_time")

      def morning(df2: DataFrame): DataFrame ={
        df2.select(col("login_id"),col("login_time"),
          when(col("login_time") < "12:00","True")
        .otherwise("False").alias("is_morning"))
      }

      morning(df1).show()
    } catch {
      case e: Exception => println("Error Message :"+e.getMessage)
    } finally {
      spark.stop()
    }
  }

}
