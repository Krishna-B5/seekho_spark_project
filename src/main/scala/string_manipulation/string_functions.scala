package string_manipulation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat, concat_ws, instr, length, lit, lower, lpad, ltrim, regexp_extract, regexp_replace, reverse, rpad, rtrim, split, substring, substring_index, translate, trim, upper, when}

object string_functions {

  def main(args:Array[String]): Unit ={

    val spark = SparkSession.builder()
      .appName(" This program explains all string functions")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //concat()
    val df = Seq(("Hello","World")).toDF("col1","col2")
    df.select(concat(col("col1"),lit(" "), col("col2")).alias("greeting")).show()

    //concat_ws()
    val df1 = Seq(("Hello","World")).toDF("col1","col2")
    df1.select(concat_ws("    ",col("col1"),col("col2")).alias("greeting")).show()

    //lenght()
    df1.select(length(col("col1")).alias("Length")).show()

    // upper or lower
    df1.select(upper(col("col1")).alias("upper_case")).show()
    df1.select(lower(col("col2")).alias("lower_case")).show()

    // trim, ltrim, rtrim
    val df2 = Seq(("    Hello    ")).toDF("col3")
    df2.select(trim(col("col3")).alias("trim_space")).show()
    df2.select(ltrim(col("col3")).alias("ltrim_space")).show()
    df2.select(rtrim(col("col3")).alias("rtrim_space")).show()

    // substring
    df1.select(substring(col("col1"),2,3).alias("substr") ).show()

    // substring_index
    val df4 = Seq(("Hello world Krishna")).toDF("col1")
    df4.select(substring_index(col("col1"), ".", 2).as("sub_index")).show() // Up to the second dot

    // split
    df4.select(split(col("col1")," ").getItem(0).alias("splited")).show(false)

    // regexp_extract
    val df5 = Seq(("123abc")).toDF("col1")
    df5.select(regexp_extract(col("col1"), "([0-9]+)", 1).as("numbers")).show()

    //regexp_replace
    df5.select(regexp_replace(col("col1"),"[0-9]","X").alias("replaced")).show()

    //instr
    val df6 = Seq(("Hello World World World")).toDF("col1")
    df6.select(instr(col("col1"), "World").as("pos")).show()

    // replace

//    df6.selectExpr("repalce(col1,"World","Spark").as
    df6.select(regexp_replace(col("col1"),"World","Spark").alias("replaced")).show()

    // translate
    val df7 = Seq(("123-456-7890")).toDF("col1")
    df7.select(translate(col("col1"), "1234567890", "ABCDEFGHIJ").as("translated")).show()

    // lpad or rpad

    df5.select(lpad(col("col1"),15,"*").alias("pad1_str"),rpad(col("col1"),15,"$").alias("pad2_str")).show()

    df5.select(reverse(col("col1")).alias("reversed")).show()



  }

}
