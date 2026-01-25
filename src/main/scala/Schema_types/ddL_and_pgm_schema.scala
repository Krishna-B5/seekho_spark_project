package Schema_types

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object ddL_and_pgm_schema {

  def main(agrs:Array[String]):Unit = {

    print("This prg explain DDL and pgmmatic schemas - Explicit Schema")

    val spark = SparkSession.builder()  // creating a SparkSession
      .appName("DDL_AND_PRG_SCHEMA")
      .master("local[*]")
      .getOrCreate()
// we will be creating the explicit schema on top of DF.
//    val ddl_schema = "Id Int,Name String,Age Int,Salary Int" // DDL Schema

    val pgm_schema = StructType( List(                        //pgm Schema
      StructField("ID",IntegerType,nullable = true),
      StructField("Name",StringType,nullable = true),
      StructField("Age",IntegerType,nullable = true),
      StructField("Salary",IntegerType,nullable = true)
    ) )

    val df = spark.read                // reading and loading the data
      .format("CSV")
      .option("header",value = true)
      .schema(pgm_schema)             // Explicit schemas we are creating a schema on top of DF
//      .option("mode","PERMISSIVE") // By default if u not added also it will consider
//      .option("mode","FAILFAST")   // It will throw a message Malformed records are detected....
//      .option("mode","DROPMALFORMED") // It will drop the corrupted rows.
      .option("path","C:/Users/91973/Desktop/Practise_files/emp.csv")
      .load()

    df.show()               // displaying the data

    df.printSchema()        // Printing the Schema

  }

}
