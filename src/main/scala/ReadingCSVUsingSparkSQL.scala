import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark._



object ReadingCSVUsingSparkSQL {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder
      .master("local")
      .appName("spark session example")
      .getOrCreate()




    ss.sparkContext.setLogLevel("WARN")



    // Loading Data From MS Excel File into Dataframe  Using SparkSQl CSV loader function
    val csvDF = ss.read
      .option("header", "false")
      .csv("src\\main\\resource\\ride.csv")



    // Display Content of Data Franme
    //csvDF.show()


    /* Out_Put

             +------------------+---------------+--------+-----------------+---------------+-----+------------------+
             |     """START_DATE|       END_DATE|CATEGORY|            START|           STOP|MILES|        PURPOSE"""|
             +------------------+---------------+--------+-----------------+---------------+-----+------------------+
             | """1/1/2016 21:11| 1/1/2016 21:17|Business|      Fort Pierce|    Fort Pierce|  5.1| Meal/Entertain"""|
             |  """1/2/2016 1:25|  1/2/2016 1:37|Business|      Fort Pierce|    Fort Pierce|    5|                 "|
             | """1/2/2016 20:25| 1/2/2016 20:38|Business|      Fort Pierce|    Fort Pierce|  4.8|Errand/Supplies"""|
             | """1/5/2016 17:31| 1/5/2016 17:45|Business|      Fort Pierce|    Fort Pierce|  4.7|        Meeting"""|
             | """1/6/2016 14:42| 1/6/2016 15:49|Business|      Fort Pierce|West Palm Beach| 63.7| Customer Visit"""|
             | """1/6/2016 17:15| 1/6/2016 17:19|Business|  West Palm Beach|West Palm Beach|  4.3| Meal/Entertain"""|
             | """1/6/2016 17:30| 1/6/2016 17:35|Business|  West Palm Beach|     Palm Beach|  7.1|        Meeting"""|
             | """1/7/2016 13:27| 1/7/2016 13:33|Business|             Cary|           Cary|  0.8|        Meeting"""|
             | """1/10/2016 8:05| 1/10/2016 8:25|Business|             Cary|    Morrisville|  8.3|        Meeting"""|
             |"""1/10/2016 12:17|1/10/2016 12:44|Business|          Jamaica|       New York| 16.5| Customer Visit"""|
             |"""1/10/2016 15:08|1/10/2016 15:51|Business|         New York|         Queens| 10.8|        Meeting"""|
             |"""1/10/2016 18:18|1/10/2016 18:53|Business|         Elmhurst|       New York|  7.5|        Meeting"""|
             |"""1/10/2016 19:12|1/10/2016 19:32|Business|          Midtown|    East Harlem|  6.2|        Meeting"""|
             | """1/11/2016 8:55| 1/11/2016 9:21|Business|      East Harlem|          NoMad|  6.4| Temporary Site"""|
             |"""1/11/2016 11:56|1/11/2016 12:03|Business|Flatiron District|        Midtown|  1.6|Errand/Supplies"""|
             |"""1/11/2016 13:32|1/11/2016 13:46|Business|          Midtown|   Midtown East|  1.7| Meal/Entertain"""|
             |"""1/11/2016 14:30|1/11/2016 14:43|Business|     Midtown East|        Midtown|  1.9| Meal/Entertain"""|
             |"""1/12/2016 12:33|1/12/2016 12:49|Business|          Midtown|  Hudson Square|  1.9| Meal/Entertain"""|
             |"""1/12/2016 12:53|1/12/2016 13:09|Business|    Hudson Square|Lower Manhattan|    4| Meal/Entertain"""|
             |"""1/12/2016 14:42|1/12/2016 14:56|Business|  Lower Manhattan|  Hudson Square|  1.8|Errand/Supplies"""|
             +------------------+---------------+--------+-----------------+---------------+-----+------------------+
             only showing top 20 rows

             */


    //Creating table from Dataframe
    csvDF.createOrReplaceTempView(" my_drive")

    // Retrive Data From Table using sql()
    //val results = ss.sql(" Select * from my_drive" )
   // results.show()


   //ss.sql("select SUBSTR(LTRIM(_c0),4,15)  AS Start_Time, SUBSTR(TRIM(_c1),0,15) AS End_Time from my_drive").


    val sqlRDD =ss.sql("select SUBSTR(LTRIM(_c0),4,15)  AS Start_Time, SUBSTR(TRIM(_c1),0,15) AS End_Time from my_drive").rdd
    sqlRDD.foreach(println)

    /*val dateFormat = sqlRDD.map( x=> {
        val s_time =x.split(",")(0)
        val s_day   = s_time.split("/")(0)
        val s_month = s_time.split("/")(1)
        val s_year  = s_time.split("/")(2)
        (s_year+"-"+s_month+"-"+s_day)
        })
dateFormat.foreach(println)

*/


    /* Out_Put */


    /* Getting No Of Trip For each catagory */
    //val  No_Of_Visit_Per_Cat = ss.sql("Select CATEGORY , count(CATEGORY) from my_drive  group by CATEGORY ")
    //No_Of_Visit_Per_Cat.show()

    /* Out_Put
             +--------+---------------+
             |CATEGORY|count(CATEGORY)|
             +--------+---------------+
             |Personal|             77|
             |Business|           1078|
             +--------+---------------+

             */


    /* Total mile covered */
    //ss.sql("select COUNT(MILES) as ToTal_Distance_Covered_In_Miles ,AVG(MILES) as Average_Distance_Covered_In_Miles  from my_drive").show()

    /*Out_put
             +-------------------------------+---------------------------------+
             |ToTal_Distance_Covered_In_Miles|Average_Distance_Covered_In_Miles|
             +-------------------------------+---------------------------------+
             |                           1155|               10.566839826839812|
             +-------------------------------+---------------------------------+
           } */


    /* Gettig Total Traveling Hour */
   // val sqlResult = ss.sql(" select ")

    //sqlResult.map(rec=> rec(0).toString )
     //sqlResult.show()
    //val sqlRDD = sqlResult.rdd
    //sqlRDD.foreach(println)



  }
}
