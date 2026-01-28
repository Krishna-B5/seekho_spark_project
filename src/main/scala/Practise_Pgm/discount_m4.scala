package Practise_Pgm

import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object discount_m4 {

  def main(args:Array[String]):Unit ={

    val spark = SparkSession.builder()
      .appName("Finding the discount")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    try{
      val data = List((1,100),(2,1500),(3,300))

      val df = spark.createDataFrame(data)
      val df1 = df.toDF("sale_id","amount")

      def discount(df2: DataFrame): DataFrame ={
        df2.select(col("sale_id"),col("amount"),
          when(col("amount") < 200,0)
            .when(col("amount") between(200, 1000),10)
            .otherwise(20).alias("Discount") )
      }

      discount(df1).show()


    }catch {
      case e: Exception => print("Error Message :"+e.getMessage)
    } finally {
      spark.stop()
    }
  }

}
