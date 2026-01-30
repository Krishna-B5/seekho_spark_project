package Practise_Pgm

import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object order_type_c5 {

  def main(args:Array[String]): Unit={

    val spark = SparkSession.builder()
      .appName(" Order type cheap, bulk")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    try{

      val data = List( (1, 5, 100), (2, 10, 150), (3, 20, 300) )

      val df = spark.createDataFrame(data)
      val df1 = df.toDF("order_id","quantity","total_price")

      def order_types(df2: DataFrame): DataFrame ={
        df2.withColumn("order_type",when(col("quantity") < 10 && col("total_price") < 200,"Small & Cheap")
        .when(col("quantity") >= 10 && col("total_price") < 200,"Bulk & Discounted")
        .otherwise("Premium order") )
      }

      order_types(df1).show()

    } catch {
      case e: Exception => println("Error Message : "+e.getMessage)
    } finally {
      spark.stop()
    }
  }

}
