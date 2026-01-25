package how_to_select

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object when_conditions {

  def main(args:Array[String]):Unit ={

    println(" This pgm is about when conditions :")

    val spark = SparkSession.builder()      // spark session is created
      .appName("When_Conditions")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read                   // reading and loading the data to DF
      .format("csv")
      .option("header", value = true)
      .option("path","C:/Users/91973/Desktop/Practise_files/emp.csv")
      .load()

    val df1 = df.select(
      col("ID"),
      col("Name"),
      col("Age"),
      col("Salary"),
      when(col("Age") > 31,"Eligible")
        .otherwise("Not Eligible").alias("status")
    )

    df1.show()
    df1.printSchema()

    val df2 = df.select(
      col("ID"),
      col("Name"),
      col("Age"),
      col("Salary"),
      when(col("Age") > 40,"Pre-Eligible")
        .when(col("Age") > 30 && col("Age") <40,"Eligible" )
        .otherwise("Not Eligible").alias("status")
    )

    df2.show()
  }

}
