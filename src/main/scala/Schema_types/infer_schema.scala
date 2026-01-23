package Schema_types

import org.apache.spark.sql.SparkSession

object infer_schema {

  def main(args:Array[String]):Unit ={

    print(" This prg explains InferSchema")

    val spark = SparkSession.builder()  //  creating a SparkSession
      .appName("InferSchema")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read   //  Reading a CSV file
      .format("CSV")
      .option("header","true")
      .option("inferschema","true")
      .option("path","C:/Users/91973/Desktop/Practise_files/emp.csv")
      .load()

    df.show()   //  displaying the data

    df.printSchema()     //  printing schema
  }
}
