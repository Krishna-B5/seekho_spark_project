package Schema_types

import org.apache.spark.sql.SparkSession

object implicit_schema {

  def main(args:Array[String]):Unit ={

    print(" This prgm will explain about implicit schema")

    val spark = SparkSession.builder()
      .appName("Implicit_Schema")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .format("json")
      .option("multiline",true)
      .option("path","C:/Users/91973/Desktop/Practise_files/colors.json")
      .load()

    df.show()

    df.printSchema()
  }

}
