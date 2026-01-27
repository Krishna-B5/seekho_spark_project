package Practise_Pgm

import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.{col, when}

object high_med_low {

  def main(args:Array[String]):Unit={

    val spark = SparkSession.builder()
      .appName("High Medium Low")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    try{
      val transactions = List((1,1000),(2,200),(3,5000))

      val df = spark.createDataFrame(transactions)
      val df1 = df.toDF("t_id","amount")

      def check_status(df2: DataFrame): DataFrame = {
        df2.select(col("t_id"), col("amount"),
          when(col("amount") > 1000,"High")
          .when(col("amount") between(500, 1000),"Medium")
          .otherwise("Low").alias("Status")
        )
      }

      check_status(df1).show()

      df1.createOrReplaceTempView("transaction")
      println(" SQL Output ")
      spark.sql(
        """ select *,
          case
          when amount > 1000 then "High"
          when amount between 500 and 1000 then "Medium"
          else "low"
          end as status
          from transaction""").show()

    }catch {
      case e: Exception => println("Error Message :" +e.getMessage)
    }finally {
      spark.stop()
    }
  }
}
