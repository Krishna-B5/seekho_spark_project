package Practise_Pgm

import org.apache.spark.sql.functions.{col, regexp_extract, split}
import org.apache.spark.sql.{DataFrame, SparkSession}

object email_m2 {

  def main(args:Array[String]):Unit ={

    val spark = SparkSession.builder()
      .appName("Email Yahoo")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    try{

      val data = List((1, "john@gmail.com"), (2, "jane@yahoo.com"), (3, "doe@hotmail.com"))

      val df = spark.createDataFrame(data)
      val df1 = df.toDF("cust_id","email")

      def fetch_email(df2: DataFrame): DataFrame = {
        df2.withColumn("email_provide",split(split(col("email"),"@").getItem(1), "\\.").getItem(0) )
      }
 
      fetch_email(df1).show()

      df1.createOrReplaceTempView("sample")
      spark.sql(
        """ select *,
          regexp_substr(email, '@([^\\.]+)',1,1,NULL,1) as email_doamain
          from sample""").show()

    } catch {
      case e: Exception => println("Error Message :"+e.getMessage)
    } finally {
      spark.stop()
    }
  }
}
