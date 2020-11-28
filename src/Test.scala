
import org.apache.spark._
import org.apache.spark.SparkContext


object Test {
  def main(args: Array[String]) = {
        println("Hello, world");
        var sc=new SparkContext("local[*]","RatingsCounter");
        val lines =sc.textFile("C:/Users/med19/Desktop/Mohamed/INPT/Spark_Scala/Data/ml-latest-small");
        println("----------gooooood--------------------");
        val ratings =lines.map(x=>x.toString().split("\t")(0));
        val results =ratings.countByValue();
        val sortedResults=results.toSeq.sortBy(_._1);
        sortedResults.foreach(println);
        
        
    }
  
} 