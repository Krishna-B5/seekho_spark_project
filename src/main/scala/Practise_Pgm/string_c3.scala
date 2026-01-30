package Practise_Pgm

import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object string_c3 {

  def main(args:Array[String]): Unit ={

   val spark = SparkSession.builder()
     .appName("String content category")
     .master("local[*]")
     .getOrCreate()

    import spark.implicits._

    try{

      val data = List((1, "The quick brown fox"), (2, "Lorem ipsum dolor sit amet"),
        (3, "Spark is a unified analytics engine"))

      val df = spark.createDataFrame(data)
      val df1 = df.toDF("doc_id","content")

      def content_cat(df2: DataFrame): DataFrame ={
        df2.withColumn("content_category",when(col("content").contains("fox"),"Animal Related")
        .when(col("content").contains("Lorem"),"Placeholder Text")
        .when(col("content").contains("Spark"),"Tech Related"))
      }

      content_cat(df1).show(false)

//      val df3 = df1.filter(col("content").startsWith("T")).show()

      df1.createOrReplaceTempView("sample")
      println("SQL Logic")
      spark.sql(
        """ select *,
          case
            when content like "%fox%" then "Animal Related"
            when content like "%lorem%" then "Placeholder Text"
            when content like "%Spark%" then "Tech related"
            else NULL
          end as content_category
          from sample """).show()

    } catch {
      case e: Exception => println("Error Message : "+e.getMessage)
    } finally {
      spark.stop()
    }

  }
}
