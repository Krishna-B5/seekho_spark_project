package Practise_Pgm

import org.apache.spark.sql.functions.{col,when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Inventory_M1 {

  def main(args:Array[String]):Unit={

    val spark = SparkSession.builder()
      .appName("Inventory stock")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    try{

        val data = List((1,5),(2,15),(3,25))

        val df = spark.createDataFrame(data)
        val df1 = df.toDF("item_id","quantity")

        def inventory_list(df2: DataFrame): DataFrame = {
          df2.select(col("item_id"),col("quantity"),
            when(col("quantity") < 10,"Low")
          .when(col("quantity") between(10, 20),"Medium")
          .otherwise("High").alias("stock_level") )
        }

        inventory_list(df1).show()

      df1.createOrReplaceTempView("sample")
      println("SQL Logic")
      spark.sql(
        """ select *,
          case
          when quantity < 10 then "low"
          when quantity between 5 and 10 then "medium"
          else "High"
          end as stock_level
          from quantity """).show()

    }catch {
      case e: Exception => println(" Error message :"+e.getMessage)
    }finally {
      spark.stop()
    }
  }

}
