package how_to_select

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, concat, current_date, floor, initcap, lit, months_between, to_date, when}


object stand_class_pgm {

  def main(args:Array[String]): Unit ={

    val spark = SparkSession.builder()
      .appName("Initcap class prm")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    try{
      val data = List( ("karthik", "Data", 130000, "2017-06-10", Some(4.7)),
        ("pratik", "QA", 85000, "2020-01-15", Some(3.8)),
        ("veer", "Data", 60000, "2022-09-01", None),
        ("veena", "HR", 95000, "2019-03-20", Some(2.9) ))

      val df = spark.createDataFrame(data)
      val df1 = df.toDF("name", "dept", "salary", "join_date", "rating")

      def standardize(df2: DataFrame): DataFrame ={
        df2.select(col("name"), col("dept"), col("salary"), col("join_date"), col("rating"),
          concat(initcap(col("name")),
          lit("_"),
          when(col("name").contains("karthik"),"KT")
            .when(col("name").contains("pratik"),"PR")
            .when(col("name").contains("veer"),"VR")
            .when(col("name").contains("veena"),"VN")
          .otherwise("")).alias("Name_code"),
          floor(months_between(current_date(),to_date(col("join_date"))) / 12 ).alias("experience_year"),
          when(col("salary") >= 120000, "HIGH")
            .when(col("salary").between(80000, 119999), "MEDIUM")
            .otherwise("LOW").alias("salary_band"),
          when(col("rating").isNull,"NOT_RATED")
            .when(col("rating") >= 4.5 and floor(months_between(current_date(),to_date(col("join_date"))) / 12 ) >=5, "EXCELLENT")
            .when(col("rating").between(3, 4.4),"GOOD")
            .otherwise("AVERAGE").alias("Performance_Category")
        )
      }
      standardize(df1).show()

      df1.createOrReplaceTempView("sample")
      println("SQL Logic")
      spark.sql(
        """ select name,dept,salary,join_date,rating,
            concat(
          initcap(name),
          case
            when name = "karthik" then "_KT"
            when name = "pratik" then "_PR"
            when name = "veer" then "_VR"
            when name = "veena" then "_VN"
            else ""
          end) as name_code,
          floor(months_between(current_date(),to_date(join_date) ) / 12 ) as experience_year,
          case
            when salary >= 120000 then "HIGH"
            when salary between 80000 and 119999 then "MEDIUM"
            else "LOW"
           end as salary_band,
          case
            when rating != NULL then "NOT_RATED"
            when rating >= 4.5 and floor(months_between(current_date(),to_date(join_date)) / 12 ) >=5 then "EXCELLENT"
            when rating between 3 and 4.4 then "GOOD"
            else "AVERAGE"
           end as Performance_Category
          from sample """).show()
      } catch {
      case  e:Exception => println("Error Message : "+e.getMessage)
    } finally {
      spark.stop()
    }
  }
}
