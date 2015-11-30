package spark.theshop;

import org.apache.hadoop.io.IntWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RecommGoodsRead {
	final static String HDFS_URL = "hdfs://elastic:9000";
	
	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.err.println("Usage: Order <file>");
			System.exit(1);
		}

		SparkConf sparkConf = new SparkConf().setAppName("Theshop Order").setMaster("local[*]")
				.set("es.nodes", "192.168.34.181")
        		.set("es.port", "9200")
        		.set("es.index.auto.create", "true")
        		.set("es.nodes.discovery", "false")
        		.set("es.batch.size.entries", "0");
        		
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaPairRDD<IntWritable, MyWritable> recommData = sc.sequenceFile(HDFS_URL + args[0], IntWritable.class, MyWritable.class);
		
		sc.stop();
	}
}