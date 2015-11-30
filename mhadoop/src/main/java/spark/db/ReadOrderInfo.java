package spark.db;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import scala.Tuple2;
import spark.OrderInfoModel;

public class ReadOrderInfo {
	public static void main(String[] args){
		 JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Read Object"));
	
	     JavaPairRDD<String, OrderInfoModel> pair = JavaPairRDD.<String, OrderInfoModel>fromJavaRDD(sc.<Tuple2<String, OrderInfoModel>>objectFile(args[0]));
	     
	     //JavaPairRDD<String,Tuple2<String,OrderInfoModel>> pair = JavaPairRDD.<String,Tuple2<String,OrderInfoModel>>fromJavaRDD(sc.<Tuple2<String,Tuple2<String,OrderInfoModel>>>objectFile(args[0]));
	     
	     JavaPairRDD<String, Iterable<OrderInfoModel>> gpairs = pair.groupByKey();
	     
	     
	     gpairs.saveAsTextFile(args[1]);
	     
	}
}
