package Practise_Pgm

import org.apache.spark.sql.functions.{col, current_date, date_sub, dayofmonth, last_day, next_day, to_date, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object young_c1 {

  def main(args:Array[String]):Unit ={

    val spark = SparkSession.builder()
      .appName("Based on age salary")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    try{

      val da = Seq(("123abc"))

      val data = List((1, 25, 30000), (2, 45, 50000), (3, 35, 40000))

      val df = spark.createDataFrame(data)
      val df1 = df.toDF("emp_id","age","salary")

      def age_salary(df2: DataFrame): DataFrame ={
        df2.withColumn("category",when((col("age") < 30) && (col("salary") < 35000),"Young & Low Salary")
        .when(col("age").between(30, 40) && col("salary").between(35000, 45000),"Middle Aged and Medium Salary")
        .otherwise("Old and high Salary") )
      }

      age_salary(df1).show(false)

    }catch {
      case e: Exception => println("Error Message :"+e.getMessage)
    } finally {
      spark.stop()
    }
  }
}
