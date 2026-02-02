package how_to_select

import org.apache.spark.sql.functions.{col, regexp_extract, split}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object regex_2 {

  def main(args:Array[String]): Unit ={

    val spark = SparkSession.builder()
      .appName("Regex examples")
      .master("local[*]")
      .getOrCreate()
    try{
      val data = List(("P001","A123-456-789","2024-04-01","electronics"),
        ("P002","B234-567-891","2024-05-10","home_appliance"),
        ("P003","X345-678-999", "2024-06-15", "furniture"))

      val df = spark.createDataFrame(data)
      val df1 = df.toDF("product_id","product_code","release_date","category")

      def reg1(df2: DataFrame): DataFrame ={
        df2.select(col("*"),
          regexp_extract(col("product_code"),"([A-Za-z0-9]+)",1).alias("brand_code"),
          regexp_extract(col("product_code"),"-([A-Za-z0-9]+)",1).alias("category_code"),
          regexp_extract(col("product_code"),"([A-Za-z0-9]+)$",1).alias("serial_number"),
          split(col("product_code"),"-").getItem(0).alias("s_brand_code"),
          split(col("product_code"),"-").getItem(1).alias("s_category_code"),
          split(col("product_code"),"-").getItem(2).alias("s_serial_number")
        )
      }
      reg1(df1).show()
    } catch {
      case e: Exception => println("Error Message :"+e.getMessage)
    } finally {
      spark.stop()
    }

  }

}
