package how_to_select

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object how_to_select {

  def main(args:Array[String]):Unit ={

    val spark = SparkSession.builder()  // creating a spark session
      .appName("How_to_Select_col")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read               // reading and loading the file
      .format("CSV")
      .option("header", value = true)
      .option("path","C:/Users/91973/Desktop/Practise_files/emp.csv")
      .load()

    println("Selecting single column")
    val df1 = df.select(col("ID"))
    df1.show()
    println("Selecting multiple columns")
    val df2 = df.select(col("ID"),col("Name")).show()
  }

}
