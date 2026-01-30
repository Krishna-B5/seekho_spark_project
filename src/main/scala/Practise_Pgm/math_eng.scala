package Practise_Pgm

import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object math_eng {

  def main(args:Array[String]): Unit ={

    val spark = SparkSession.builder()
      .appName("Math and English grades")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    try{

      val data = List( (1, 85, 92), (2, 58, 76),  (3, 72, 64))

      val df = spark.createDataFrame(data)
      val df1 = df.toDF("student_id","math","english")

      def eng_math(df2: DataFrame): DataFrame ={
        df2.withColumn("math_grade",when(col("math") > 80, "A")
        .when(col("math").between(60, 80), "B")
        .otherwise("c"))
          .withColumn("english_grade",when(col("english") > 80, "A")
          .when(col("english").between(60, 80),"B")
          .otherwise("c") )
      }

eng_math(df1).show()

    } catch {
      case e: Exception => println("Error Message :"+e.getMessage)
    } finally {}
    spark.stop()
  }

}
