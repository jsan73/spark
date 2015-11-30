package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import java.util.*;
/*
A Standard Top Count
*/

public class GoodsOrderCount{


    public static final String delimiters = " \t,;.?!-:@[](){}_*/";
    
    public static void main(String[] args){
        if (args.length < 2){
            System.err.println("Usage: WordCount <input_data_path> <output_data_path>");
            System.exit(1);
        }
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("WordCount"));

        JavaRDD<GoodsOrder> words = sc.textFile(args[0]).flatMap(new FlatMapFunction<String, GoodsOrder>(){

            public Iterable<GoodsOrder> call(String value){
            	String[] columns = value.split(",");
            	List<GoodsOrder> context = new ArrayList<GoodsOrder>();
            	if(columns.length <= 10) {
	                
	                GoodsOrder goodsOrder = new GoodsOrder();
	                goodsOrder.goodsName = columns[5];
	                goodsOrder.orderQty = Integer.parseInt(columns[6]);
	                context.add(goodsOrder);
	                
            	}
            	return context; 
            }
        });
        
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<GoodsOrder, String, Integer>(){
            
			public Tuple2<String, Integer> call(GoodsOrder goodsOrder)
					throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, Integer>(goodsOrder.goodsName, goodsOrder.orderQty);
			}
        });

        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>(){
            
            public Integer call(Integer a, Integer b){ 
                return a + b;
            }
        });
        
        JavaPairRDD<Integer, String> swapped = counts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>(){
           
           public Tuple2<Integer, String> call(Tuple2<String, Integer> pair){
               return pair.swap();
           }
        });
        
        JavaPairRDD<Integer, String> tops = swapped.sortByKey(false);
        
        tops.saveAsTextFile(args[1]);
        sc.stop();
    }
}