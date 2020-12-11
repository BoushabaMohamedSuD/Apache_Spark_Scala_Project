
import org.apache.spark._

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import breeze.macros.expand.args
import scala.collection.Seq
import java.lang.Integer

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType

import org.apache.spark.sql.types.StringType

/*
import com.typesafe.config.ConfigFactory
import java.io.File
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.rdd.EsSpark
import org.json4s.JsonDSL._
import java.text.SimpleDateFormat
import org.joda.time.{DateTime, Days}*/

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


    /*val model = als.fit(ratings_Format);



    val userID:Int=args(0).toInt;

    val users=Seq(userID).toDF("userId");
    val ratingsProb=model.recommendForUserSubset(users, 10);

    println(ratingsProb);
    */
    
    
    
    //////////////////////// PART 2 ///////////////////////////////////////////////
    
    
    
    //////Q1 & Q2
    
    
    val data = ss.read.option("header", "true").csv("C:/Users/med19/Desktop/Mohamed/INPT/Spark_Scala/Data/sample_data_final_wh.txt");
    
    data.show();
    data.printSchema();
    
    ////// Q3
    data.cache().show();
    println(data.rdd.getNumPartitions);
    
    
    
    ///// Q4
    
    println("*******  view top  2 rows  **************");
    
    data.show(2);
    
    
    
    //////  Q5 
    
    
      val sparkSQL = SparkSession
      .builder
      .appName("retings")
      .master("local[*]")
      .getOrCreate();

    import sparkSQL.implicits._
    
    
    
    ////////Q6 
    
    println("*********** Q6 *************");
    val schema = new StructType()
              .add("age",IntegerType,true)
              .add("sanguin",StringType,true)
              .add("ville",StringType,true)
              .add("gender",StringType,true)
              .add("id",IntegerType,true);
    
    val dataSQL= sparkSQL.read
    .schema(schema)
    .csv("C:/Users/med19/Desktop/Mohamed/INPT/Spark_Scala/Data/sample_data_final_wh.txt")
    .cache();
   
    
    //dataSQL.randomSplit([1.0, 2.0,0.6,0.5,1.3]);
    
    dataSQL.show();
    
    
    ////// Q7
    
    dataSQL.printSchema();
    
    
    
    
    //////// Q8 
    
    dataSQL.show(5);
    
    
    
    
    ///// Q9 && Q10
    
    val countGender= dataSQL.groupBy("gender").count();
    countGender.show();
    
    
    
    //// Q11
    
    val count = countGender.select("count").show();
    
    
    /// Q12  
    
    val averageAge=dataSQL.groupBy("age").avg("age");
    averageAge.show();
    
    
    
    
    
    ///////               PART 3 //////////////////////////////////
    
    
    
    /*
    
    
     val conf = ConfigFactory.load()

    // Get the OAuth credentials from the configuration file
    val credentials = conf.getConfig("oauthcredentials")
    val consumerKey = credentials.getString("consumerKey")
    val consumerSecret = credentials.getString("consumerSecret")
    val accessToken = credentials.getString("accessToken")
    val accessTokenSecret = credentials.getString("accessTokenSecret")

    // Accept Twitter Stream filters from arguments passed while running the job
    val filters = if (args.length == 0) List() else args.toList

    // Print the filters being used
    println("Filters being used: " + filters.mkString(","))

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generat OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    // Initialize a SparkConf with all available cores
    val sparkConf = new SparkConf().setAppName("TwitterPopularTags").setMaster("local[*]")
    // Automatically create index in Elasticsearch
    sparkConf.set("es.resource","sparksender/tweets")
    sparkConf.set("es.index.auto.create", "false")
    // Define the location of Elasticsearch cluster
    sparkConf.set("es.nodes", "localhost")
    sparkConf.set("es.port", "9200")

    // Create a StreamingContext with a batch interval of 10 seconds.
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    // Create a DStream the gets streaming data from Twitter with the filters provided
    val stream = TwitterUtils.createStream(ssc, None, filters)

    // Process each tweet in a batch
    val tweetMap = stream.map(status => {

      // Defined a DateFormat to convert date Time provided by Twitter to a format understandable by Elasticsearch

    val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZ")

    def checkObj (x : Any) : String  = {
        x match {
          case i:String => i
          case _ => ""
        }
      }


      // Creating a JSON using json4s.JSONDSL with fields from the tweet and calculated fields
      val tweetMap =
        ("UserID" -> status.getUser.getId) ~
          ("UserDescription" -> status.getUser.getDescription) ~
          ("UserScreenName" -> status.getUser.getScreenName) ~
          ("UserFriendsCount" -> status.getUser.getFriendsCount) ~
          ("UserFavouritesCount" -> status.getUser.getFavouritesCount) ~
          ("UserFollowersCount" -> status.getUser.getFollowersCount) ~
          // Ration is calculated as the number of followers divided by number of people followed
          ("UserFollowersRatio" -> status.getUser.getFollowersCount.toFloat / status.getUser.getFriendsCount.toFloat) ~
          ("Geo_Latitude" -> checkObj(status.getGeoLocation.getLatitude)) ~
          ("Geo_Longitude" -> checkObj(status.getGeoLocation.getLongitude)) ~
          ("UserLang" -> status.getUser.getLang) ~
          ("UserLocation" -> status.getUser.getLocation) ~
          ("UserVerification" -> status.getUser.isVerified) ~
          ("UserName" -> status.getUser.getName) ~
          ("UserStatusCount" -> status.getUser.getStatusesCount) ~
          // User Created DateTime is first converted to epoch miliseconds and then converted to the DateFormat defined above
          ("UserCreated" -> formatter.format(status.getUser.getCreatedAt.getTime)) ~
          ("Text" -> status.getText) ~
          ("TextLength" -> status.getText.length) ~
          //Tokenized the tweet message and then filtered only words starting with #
          ("HashTags" -> status.getText.split(" ").filter(_.startsWith("#")).mkString(" ")) ~
          ("StatusCreatedAt" -> formatter.format(status.getCreatedAt.getTime)) ~
          ("PlaceName" -> checkObj(status.getPlace.getName)) ~
          ("PlaceCountry" -> checkObj(status.getPlace.getCountry))

      // This function takes Map of tweet data and returns true if the message is not a spam
      def spamDetector(tweet: Map[String, Any]): Boolean = {
        {
          // Remove recently created users = Remove Twitter users who's profile was created less than a day ago
          Days.daysBetween(new DateTime(formatter.parse(tweet.get("UserCreated").mkString).getTime),
            DateTime.now).getDays > 1
        } & {
          // Users That Create Little Content =  Remove users who have only ever created less than 50 tweets
          tweet.get("UserStatusCount").mkString.toInt > 50
        } & {
          // Remove Users With Few Followers
          tweet.get("UserFollowersRatio").mkString.toFloat > 0.01
        } & {
          // Remove Users With Short Descriptions
          tweet.get("UserDescription").mkString.length > 20
        } & {
          // Remove messages with a Large Numbers Of HashTags
          tweet.get("Text").mkString.split(" ").filter(_.startsWith("#")).length < 5
        } & {
          // Remove Messages with Short Content Length
          tweet.get("TextLength").mkString.toInt > 20
        } & {
          // Remove Messages Requesting Retweets & Follows
          val filters = List("rt and follow", "rt & follow", "rt+follow", "follow and rt", "follow & rt", "follow+rt")
          !filters.exists(tweet.get("Text").mkString.toLowerCase.contains)
        }
      }
      // If the tweet passed through all the tests in SpamDetector Spam indicator = FALSE else TRUE
      spamDetector(tweetMap.values) match {
        case true => tweetMap.values.+("Spam" -> false)
        case _ => tweetMap.values.+("Spam" -> true)
      }

    })

    //tweetMap.print
    tweetMap.map(s => List("Tweet Extracted")).print

    // Each batch is saved to Elasticsearch with StatusCreatedAt as the default time dimension
    tweetMap.foreachRDD { tweets => EsSpark.saveToEs(tweets, "sparksender/tweets", Map("es.mapping.timestamp" -> "StatusCreatedAt")) }

    ssc.start  // Start the computation
    ssc.awaitTermination  // Wait for the computation to terminate
    */
    
    
    
    
    
    
    
    
    
    
    
    

  }

} 