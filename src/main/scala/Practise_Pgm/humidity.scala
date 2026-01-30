package Practise_Pgm

import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object humidity {

  def main(args:Array[String]): Unit ={

    val spark = SparkSession.builder()
      .appName("Humidity and Temperature")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    try{

      val data = List( (1, 25, 60), (2, 35, 40), (3, 15, 80) )

      val df = spark.createDataFrame(data)
      val df1 = df.toDF("day_id","temperature","humidity")

      def temp(df2: DataFrame): DataFrame ={
        df2.withColumn("is_hot",when(col("temperature") > 30, true)
        .otherwise(false) )
        .withColumn("is_humid",when(col("humidity") > 30,true)
        .otherwise(false) )
      }

      temp(df1).show()

    } catch {
      case e: Exception => println("Error Message : "+e.getMessage)
    } finally (
      spark.stop()
    )
  }

}
