package Practise_Pgm

import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object feedback_c2 {

  def main(args:Array[String]): Unit ={

    val spark = SparkSession.builder()
      .appName(" Feedback and Rating")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    try{

      val data = List((1,1),(2,4),(3,5))

      val df = spark.createDataFrame(data)
      val df1 = df.toDF("reveiw_id","rating")

      def feedback(df2: DataFrame): DataFrame ={
        df2.select(col("reveiw_id"),col("rating"),
          when(col("rating") < 3,"Bad")
            .when(col("rating").isin(3,4),"Good")
            .otherwise("Excellent").alias("feedback"),
          when(col("rating") >= 3,"True")
        .otherwise("False").alias("is_positive"))
//        df2.withColumn("feedback",when(col("rating") < 3,"Bad")
//        .when(col("rating").isin(3,4),"Good")
//        .otherwise("Excellent"))
//          .withColumn("is_positive",when(col("rating") >= 3,"True")
//          .otherwise("False"))
      }

      feedback(df1).show()

      df1.createOrReplaceTempView("sample")
      println("SQL Logic")

      spark.sql(
        """ select *,
          case
            when rating < 3 then "Bad"
            when rating in (3,4) then "Good"
            else "Excellent"
          end as feedback,
          case
            when rating >= 3 then true
            else false
          end as is_positive
          from sample """).show()


    }catch {
      case e: Exception => println("Error Message : "+e.getMessage)
    } finally {
      spark.stop()
    }
  }

}
