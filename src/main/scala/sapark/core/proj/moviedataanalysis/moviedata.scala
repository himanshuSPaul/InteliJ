/*
*
*
* data Structure :Year;Length;Title;Subject;Actor;Actress;Director;Popularity;Awards;*Image
* INT;INT;STRING;CAT;CAT;CAT;CAT;INT;BOOL;STRING
*
836 ->* 1953;95;How to Marry a Millionaire;Comedy;Powell, William;Monroe, Marilyn;Negulesco, Jean;65;No;NicholasCage.png

* */



package sapark.core.proj.moviedataanalysis


import org.apache.spark._
//import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession


object ReadingTextDataUsingSparkSql
       {
       def main(args: Array[String]): Unit =
           {
         val ss = SparkSession.builder
                              .master("local")
                              .appName("spark session example")
                              .getOrCreate()


         val sparkSession = SparkSession.builder
                            .master("local")
                            .appName("example")
                            .getOrCreate()


         //val data = sparkSession.read.text("src/main/resources/data.txt").as[String]
         val text = sparkSession
                    .read
                    .text("src\\main\\resource\\film_data.txt")


             text.show();


           }
       }



