package spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class JavaWordCount {
	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setAppName("Word Count");
	    JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		
		// Read the source file
	    JavaRDD<String> input = sparkContext.textFile(args[0]);

	    // RDD is immutable, let's create a new RDD which doesn't contain empty lines
	    // the function needs to return true for the records to be kept
	    JavaRDD<String> nonEmptyLines = input.filter(new Function<String, Boolean>() {
	      public Boolean call(String s) throws Exception {
	        if(s == null || s.trim().length() < 1) {
	          return false;
	        }
	        return true;
	      }
	    });

	    // Now we have non-empty lines, lets split them into words
	    JavaRDD<String> words = nonEmptyLines.flatMap(new FlatMapFunction<String, String>() {
	      public Iterable<String> call(String s) throws Exception {
	        return Arrays.asList(s.split(","));
	      }
	    });

	    // Convert words to Pairs, remember the TextPair class in Hadoop world
	    JavaPairRDD<String, Integer> wordPairs = words.mapToPair(new PairFunction<String, String, Integer>() {
	      public Tuple2<String, Integer> call(String s) {
	        return new Tuple2<String, Integer>(s, 1);
	      }
	    });

	    JavaPairRDD<String, Integer> wordCount = wordPairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
	      public Integer call(Integer integer, Integer integer2) throws Exception {
	        return integer + integer2;
	      }
	    });
	    
	    /*
	    JavaPairRDD<Integer, String> swappedPair = wordPairs.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
	           public Tuple2<Integer, String> call(Tuple2<String, Integer> item) throws Exception {
	               return item.swap();
	           }

	        }).sortByKey(true);
	        

	    
	    wordCount = swappedPair.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
	           public Tuple2<String, Integer> call(Tuple2<Integer, String> item) throws Exception {
	               return item.swap();
	           }

	        });
	    
	    */
	    
	    // Just for debugging, NOT FOR PRODUCTION
	    wordCount.foreach(new VoidFunction<Tuple2<String, Integer>>() {
	      public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
	        System.out.println(String.format("%s - %d", stringIntegerTuple2._1(), stringIntegerTuple2._2()));
	      }
	    });


	}
}
