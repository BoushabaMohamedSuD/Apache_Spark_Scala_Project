
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]) = {
    println("Hello, world");
    var sc = new SparkContext("local[*]", "RatingsCounter");
    val lines = sc.textFile("C:/Users/med19/Desktop/Mohamed/INPT/Spark_Scala/Data/ml-latest-small/links.csv");
    //println(lines);
    println("----------gooooood--------------------");
    /*val ratings =lines.map(x=>x.toString().split(",")(0));
        val results =ratings.countByValue();
        val sortedResults=results.toSeq.sortBy(_._1);
        sortedResults.foreach(println);*/

    ///// Question 1 ///////

    val links_lines = sc.textFile("C:/Users/med19/Desktop/Mohamed/INPT/Spark_Scala/Data/ml-latest-small/links.csv");
    val movies_lines = sc.textFile("C:/Users/med19/Desktop/Mohamed/INPT/Spark_Scala/Data/ml-latest-small/movies.csv");
    val ratings_lines = sc.textFile("C:/Users/med19/Desktop/Mohamed/INPT/Spark_Scala/Data/ml-latest-small/ratings.csv");
    val tags_lines = sc.textFile("C:/Users/med19/Desktop/Mohamed/INPT/Spark_Scala/Data/ml-latest-small/tags.csv");

    //// Question 2 ////////////////////

    val ss = SparkSession
      .builder
      .appName("retings")
      .master("local[*]")
      .getOrCreate();

    val ratings_lines_sql = ss.sparkContext.textFile("C:/Users/med19/Desktop/Mohamed/INPT/Spark_Scala/Data/ml-latest-small/ratings.csv");

    /// Question 3 ///////////////////////////////
  
  }

} 