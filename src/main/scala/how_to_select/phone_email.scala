package how_to_select

import org.apache.spark.sql.functions.{col, concat, lit, regexp_replace, split, when}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object phone_email {

  def main(args:Array[String]): Unit={

    val spark = SparkSession.builder()
      .appName("Phone masking and email fname and lname")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    try{

      val data = List((1,"+91 9739362333","krishna.B@gmail.com"),(2,"66 1234567890","kiran.B@gmail.com"))

      val df = spark.createDataFrame(data)
      val df1 = df.toDF("id","phone","email")

      def p_email(df2: DataFrame): DataFrame ={
        df2.select(col("id"),
          concat(
            split(col("phone")," ").getItem(0),
            lit(" "),
            regexp_replace(split(col("phone")," ").getItem(1),"^\\d{6}","******")
          ).alias("masked_phone"),
          split(split(col("email"),"@").getItem(0),"\\.").getItem(0).alias("fname"),
          split(split(col("email"),"@").getItem(0),"\\.").getItem(1).alias("lname")
        )

      }

      p_email(df1).show()
    } catch {
      case e: Exception => println("Error Message : "+e.getMessage)
    } finally {
      spark.stop()
    }
  }

}
