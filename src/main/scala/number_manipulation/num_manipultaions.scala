package number_manipulation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object num_manipultaions {

  def main(args:Array[String]): Unit ={

    val spark = SparkSession.builder()
      .appName(" Number Manipulations")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    val data = Seq((10,20))
    val df = spark.createDataFrame(data)
    val df2 = df.toDF("a","b")

    df2.select(
      (col("a") + col("b")).alias("Add"),
      (col("a") - col("b")).alias("Sub"),
      (col("a") * col("b")).alias("Mul"),
      (col("a") / col("b")).alias("Div"),
      (col("a") % col("b")).alias("Per")
    ).show()
  }

}
