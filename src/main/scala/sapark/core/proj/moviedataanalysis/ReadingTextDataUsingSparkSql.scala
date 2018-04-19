/*
*
*
* data Structure :Year;Length;Title;Subject;Actor;Actress;Director;Popularity;Awards;*Image
* INT;INT;STRING;CAT;CAT;CAT;CAT;INT;BOOL;STRING
*
836 ->* 1953;95;How to Marry a Millionaire;Comedy;Powell, William;Monroe, Marilyn;Negulesco, Jean;65;No;NicholasCage.png

* */



package sapark.sql.proj;


import org.apache.spark.sql.SparkSession


object ReadingTextDataUsingSparkSql
{
  def main(args: Array[String]): Unit =
  {
    val ss = SparkSession.builder
      .master("local")
      .appName("spark session example")
      .getOrCreate()


    val sSession = SparkSession.builder
      .master("local")
      .appName("example")
      .getOrCreate()


    sSession.sparkContext.setLogLevel("WARN")

    //val data = sparkSession.read.text("src/main/resources/data.txt").as[String]
    val text = sSession
               .read
               .text("src\\main\\resource\\film_data.txt")


    text.show();


  }
}



