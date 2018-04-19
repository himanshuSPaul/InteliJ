/* * * * WordCount * * *
 *  Here Goal is to Find top 5 Word in the given  inputs basing on there occupancy
 *  The Whole idea is to read each record of input and spliting them by white space which will result words
 *  Now each word will be converted into pair RDD  .
 * */



import org.apache.spark._
//import org.apache.spark.SparkContext._ It will be imported automatically as it will be imported while importing above package

object SparkWordCount
{
  def main(args: Array[String])
  {

    // Setting Env Parameters
    val conf = new SparkConf()
                   .setAppName("wordCount")
                   .setMaster("local[*]")

    // Create a Scala Spark Context. and passing conf object to it
    val sc = new SparkContext(conf)

    // Load our input data.
    val textRDD =  sc.textFile("src\\main\\resource\\sample_data.txt")

    // Splitting records from Input Data on White Space to get Words
    val wordsRDD = textRDD.flatMap(_.split(" "))

    // Generating Key-Value pair for each word
    val wordKeyValueRDD = wordsRDD.map(x=>(x,1))

    // Summing Values Having Same Key
    val sumByWordRDD = wordKeyValueRDD.reduceByKey((x,y)=>x+y)

    //Sorting Output basing on No Of Occurrence
    val sortingPairRDD = sumByWordRDD.sortBy(_._2)

    // Printing output on screen
    sortingPairRDD.foreach(println)

  }
}
