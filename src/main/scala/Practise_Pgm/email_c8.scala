package Practise_Pgm

import org.apache.spark.sql.functions.{col, lower, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object email_c8 {

  def main(args:Array[String]): Unit ={

    val spark = SparkSession.builder()
      .appName("Extracting Domain")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    try{

      val data = List( (1, "user@gmail.com"), (2, "admin@yahoo.com"), (3, "info@hotmail.com") )

      val df = spark.createDataFrame(data)
      val df1 = df.toDF("email_id","email_add")

      def email_add(df2: DataFrame): DataFrame ={
        df2.withColumn("email_domain",when(lower(col("email_add")).contains("gmail"),"Gmail")
        .when(lower(col("email_add")).contains("yahoo"),"Yahoo")
        .otherwise("Hotmail"))
      }

      email_add(df1).show()

    } catch {
      case e: Exception => println("Error Message :"+e.getMessage)
    } finally {
      spark.stop()
    }
  }

}
