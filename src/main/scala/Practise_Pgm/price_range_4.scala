package Practise_Pgm

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, when}

object price_range_4 {

  def main(args:Array[String]):Unit={

    val spark = SparkSession.builder()
      .appName("Price range cheap moderate expensive")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    try {

      val data = List((1,30.5),(2,150.75),(3,75.25))  // List of tuples

      val df = spark.createDataFrame(data)
      val df1 = df.toDF("product_id","price")         // create DF

      def price_ran(df: DataFrame): DataFrame = {
        df1.select(col("product_id"), col("price"),
          when(col("price") < 50, "cheap")
            .when(col("price") between(50, 100), "Moderate")
            .otherwise("Expensive").alias("price_range") )
      }

      price_ran(df1).show()

      println(" SQL Logic ")
      df1.createOrReplaceTempView("product")
      spark.sql(
        """ select *,
          case
          when price < 50 then "cheap"
          when price between 50 and 100 then "Moderate"
          else "Expensive"
          end as price_range
          from product """).show()

    }catch {
      case e: Exception => println(" Error Message is :"+e.getMessage)
    }finally {
      spark.stop()
    }
  }

}
