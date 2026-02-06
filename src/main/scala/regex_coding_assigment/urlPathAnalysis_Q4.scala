package regex_coding_assigment

import org.apache.spark.sql.functions.{avg, col, count, length, lower, regexp_extract}
import org.apache.spark.sql.{DataFrame, SparkSession}

object urlPathAnalysis_Q4 {

  def main(args:Array[String]): Unit ={

    val spark = SparkSession.builder()
      .appName("ULR Path Analysis and Extraction")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    try{

      val data = List(("S001", "https://example.com/home","2025-07-01 10:00:00","Chrome/90.0"),
        ("S002","http://sample.org/contact","2025-07-02 11:30:00","Firefox/85.0"),
        ("S003","https://example.com/admin","2025-07-03 12:45:00","Safari/14.1"),
        ("S004","https://example.com/admin","2025-07-04 13:45:00","Firefox/15.1"),
        ("S005","https://example.com/home","2025-07-05 14:45:00","Chrome/16.1"),
        ("S005","https://example.com/admin","2025-07-05 14:45:00","Chrome/16.1"))

      val df = spark.createDataFrame(data)
      val df1 = df.toDF("session_id","URL","timestamp","user_agent")

      val protocol_url = regexp_extract(col("URL"),"^([a-zA-Z:]+)",1)
      val domain_url = regexp_extract(col("URL"),"^([a-zA-Z:]+)\\//([a-zA-z.]+)",2)
      val path_url = regexp_extract(col("URL"),"(/[a-zA-z]+)$",1)

      def urlPath(df2: DataFrame): DataFrame ={
        val resultdf = df2.dropDuplicates("session_id")
          .select(col("*"),
            lower(col("user_agent")).alias("l_user_agent"),
            protocol_url.alias("protocol"),
            domain_url.alias("domain"),
            path_url.alias("path")  )
          .filter(!col("path").contains("/admin"))
          .groupBy(col("domain")).agg(
            avg(length(col("path"))).alias("avg_len"),
            count(col("session_id")).alias("ttl_session")
          )
        resultdf
      }

      urlPath(df1).show(false)
      df1.createOrReplaceTempView("sample")
      spark.sql(
        """
          with dedup as(
          select session_id,URL,timestamp,user_agent
          from sample
          group by session_id,URL,timestamp,user_agent
          ),
          derived as(
          select session_id,URL,timestamp,user_agent,
          lower(user_agent) as l_user_agent,
          regexp_extract(URL,"^([a-zA-Z:]+)",1) as protocol_url,
          regexp_extract(URL,"^([a-zA-Z:]+)\\//([a-zA-z.]+)",2) as domain_url,
          regexp_extract(URL,"(/[a-zA-z]+)$",1) as path_url
          from dedup
          )
          select
          domain_url,
          avg(length(path_url)) as avg_len,
          count(session_id) as ttl_session
          from derived
          where !(path_url == "/admin")
          group by domain_url
          """).show()

    } catch {
      case e: Exception => println("Error Message : "+e.getMessage)
    } finally {}
    spark.stop()
  }

}
