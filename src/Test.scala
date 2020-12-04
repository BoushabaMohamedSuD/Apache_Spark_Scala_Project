
import org.apache.spark._

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import breeze.macros.expand.args
import scala.collection.Seq
import java.lang.Integer

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

    import ss.implicits._

    val ratings_lines_sql = ss.read.textFile("C:/Users/med19/Desktop/Mohamed/INPT/Spark_Scala/Data/ml-latest-small/ratings.csv");
    ratings_lines_sql.show();
    /// Question 3 ///////////////////////////////

    /*
     * Collaborative algorithm uses “User Behaviour” for recommending items.
     * They exploit behaviour of other users and items in terms of transaction history, ratings, selection and purchase information.
     * Other users behaviour and preferences over the items are used to recommend items to the new users.
     *
     *
     * The point of content-based is that we have to know the content of both user and item.
     * Usually we construct user-profile and item-profile using the content of shared attribute space.
     * For example, for a movie, you represent it with the movie stars in it and the genres (using a binary coding for example).
     * For user profile, you can do the same thing based on the users likes some movie stars/genres etc.
     *
     * */

    /////// Question 4 ////////////////////////////

    /*
     *
     * for our case, we have no idea who the user is, all users have been selected randomly,
     * so the best approach system in our case is to use Collaborative Recommendation Filtering,
     *
     *
     *
     * */

    ////// Question 5//////////////////////////////

    /*
     *
     * Apache Spark ML implements alternating least squares (ALS) for collaborative filtering, a very popular algorithm for making recommendations.
		 * ALS recommender is a matrix factorization algorithm that uses Alternating Least Squares with Weighted-Lamda-Regularization (ALS-WR).
		 * It factors the user to item matrix A into the user-to-feature matrix U and the item-to-feature matrix M: It runs the ALS algorithm in a parallel fashion.
		 * The ALS algorithm should uncover the latent factors that explain the observed user to item ratings and tries ,
		 * to find optimal factor weights to minimize the least squares between predicted and actual ratings.
     *
     *
     *
     *
     * */

    //// Question 6 //////////

    /// first we are going to convert our dataset to dataframe

    // val ratings = ratings_lines_sql.map(func, encoder)

    //val header=ratings_lines_sql.first();
    //println(header);

    //ratings_lines_sql.show();

    val ratings = ss.read.option("header", "true").csv("C:/Users/med19/Desktop/Mohamed/INPT/Spark_Scala/Data/ml-latest-small/ratings.csv");

    ratings.show()

    ratings.printSchema();
    import org.apache.spark.sql.functions._
    val toInt = udf[Int, String](_.toInt)
    val toDouble = udf[Double, String](_.toDouble)

    val ratings_Format = ratings.withColumn("userId", toInt(ratings("userId")))
      .withColumn("movieId", toInt(ratings("movieId")))
      .withColumn("rating", toDouble(ratings("rating")))
      .drop("timestamp").toDF();
    
    ratings_Format.printSchema();
    ratings_Format.show();

    //val ratings =ss.read.json("C:/Users/med19/Desktop/Mohamed/INPT/Spark_Scala/Data/ml-latest-small/ratings.csv").toDF();

    // ratings.show();

      val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")


    val model = als.fit(ratings_Format);



    val userID:Int=args(0).toInt;

    val users=Seq(userID).toDF("userId");
    val ratingsProb=model.recommendForUserSubset(users, 10);

    println(ratingsProb);

  }

} 