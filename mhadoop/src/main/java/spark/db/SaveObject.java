package spark.db;

import java.io.Serializable;
import java.sql.ResultSet;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.JdbcRDD;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import scala.Tuple2;
import scala.reflect.ClassManifestFactory$;
import scala.runtime.AbstractFunction1;
import spark.OrderInfoModel;
import spark.OrderWritable;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class SaveObject implements Serializable {
	   public static final String delimiters = " \t,;.?!-:@[](){}_*/";
	    
	   private static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger.getLogger(SaveObject.class);

	    private static final JavaSparkContext sc =
	            new JavaSparkContext(new SparkConf().setAppName("OrderInfoCount").setMaster("local[*]")
	            		.set("es.nodes", "192.168.34.181")
	            		.set("es.port", "9200")
	            		.set("es.index.auto.create", "true")
	            		.set("es.nodes.discovery", "false")
	            		.set("es.batch.size.entries", "0")
	            		);

	    private static final String DRIVER_NAME = "oracle.jdbc.driver.OracleDriver";
	    private static final String CONNECTION_URL = "jdbc:oracle:thin:@10.101.91.167:1521:SHOPDB";
	    private static final String USERNAME = "DWB2B";
	    private static final String PWD = "dwb2b";
	    
	    public static void main(String[] args){
	    	
	    	DbConnection dbConnection = new DbConnection(DRIVER_NAME, CONNECTION_URL, USERNAME, PWD);

	    	String sql =  "select a.order_num, a.order_dt, a.user_key, a.pay_form, b.order_goods_num, b.goods_nm, b.order_qty, b.seller_cd, b.last_price, b.goods_cd " +
	    				  "from order_info a, order_goods b " + 
	    				  "where a.ORDER_NUM = b.ORDER_NUM " +
	    				  "AND a.order_status = 'T02' AND a.order_dt LIKE '201510%' AND a.ORDER_PATH <> '06' AND ? = ?";
	    	
	        // Load data from DB
	        JdbcRDD<Object[]> jdbcRDD =
	                new JdbcRDD<Object[]>(sc.sc(), dbConnection, sql, 1,
	                              1, 1, new MapResult(), ClassManifestFactory$.MODULE$.fromClass(Object[].class));

	        // Convert to JavaRDD
	        JavaRDD<Object[]> javaRDD = JavaRDD.fromRDD(jdbcRDD, ClassManifestFactory$.MODULE$.fromClass(Object[].class));
	        // Key-Value 조합
	        
	        JavaPairRDD<String, OrderInfoModel> pairs = javaRDD.mapToPair(new PairFunction<Object[], String, OrderInfoModel>() {
	            
				public Tuple2<String, OrderInfoModel> call(Object[] record)
						throws Exception {
					// TODO Auto-generated method stub
					OrderInfoModel order = new OrderInfoModel();
					order.setOrderNum(record[0].toString());
					order.setGoodsNm(record[5].toString());
					order.setOrderQty(Integer.parseInt(record[6].toString())*10);
					order.setGoodsPrice(Integer.parseInt(record[8].toString()));
					return new Tuple2<String, OrderInfoModel>(record[0].toString(), order);
				}
	        });
	        pairs.saveAsObjectFile(args[0]);
	        
	        
      
	        //JavaPairRDD<String, Iterable<OrderInfoModel>> gpairs = pairs.groupByKey();
	        
	        //JavaEsSpark.saveToEsWithMeta(gpairs, "theshop3/order");
	        
	        
	        //gpairs.saveAsObjectFile(args[0]);
	        
	        
	        // Join first name and last name
	        /*
	        List<String> employeeFullNameList = javaRDD.map(new Function<Object[], String>() {
	            public String call(final Object[] record) throws Exception {
	                return record[0] + " " + record[5];
	            }
	        }).collect();
	        
	        
	        for (String fullName : employeeFullNameList) {
	            //LOGGER.info(fullName);
	            System.out.println(fullName);
	        }
	        */

	        /*
	        JavaPairRDD<String, String> pairs = javaRDD.mapToPair(new PairFunction<Object[], String, String>() {
	            
				public Tuple2<String, String> call(Object[] record)
						throws Exception {
					// TODO Auto-generated method stub
					return new Tuple2<String, String>(record[0].toString(), record[5].toString());
				}
	        });
	        */
	        
	      
	        //gpairs.union(other)
	        /*
	        JavaPairRDD<String, Iterable<OrderInfoModel>> rgpairs = gpairs.reduceByKey(new Function2<Iterable<OrderInfoModel>, Iterable<OrderInfoModel>, Iterable<OrderInfoModel>>() {

				public Iterable<OrderInfoModel> call(
						Iterable<OrderInfoModel> arg0,
						Iterable<OrderInfoModel> arg1) throws Exception {
					// TODO Auto-generated method stub
					Iterator<OrderInfoModel> orderList = arg1.iterator();
					while(orderList.hasNext()) {
						System.out.println(orderList.next().getGoodsNm());
					}
					return arg1;
				}
	        	
	        });
	        */
	        //JavaEsSpark.saveToEsWithMeta(gpairs, "spark/docs");
	       // gpairs.saveAsTextFile(args[0]);
	        
	        
	       // pairs.saveAsHadoopFile(args[0], Text.class, OrderWritable.class, SequenceFileOutputFormat.class);
	        
	        /*
	         * 괄호없이 사용
	        JavaRDD<String> map = javaRDD.map(new Function<Object[], String>() {

				public String call(Object[] record) throws Exception {
					// TODO Auto-generated method stub
					return record[0] + " " + record[5];
				}
	            
				
	        });
	        
	        map.saveAsTextFile(args[0]);
	        */
	        
	        //sc.stop();
	        //JavaEsSpark.saveToEs(javaRDD, "spark/docs");
	        
/*
 
  JavaPairRDD<String, String> pairs2 = lines.mapToPair(keyData);
	        JavaPairRDD<String, String> orgRDD = sc.textFile(args[0]).flatMapToPair(new PairFlatMapFunction<String, String, String>(){
				public Iterable<Tuple2<String, String>> call(String paramT)
						throws Exception {
					List<Tuple2<String, String>> context = new ArrayList<Tuple2<String, String>>();
					System.out.println(paramT);
					context.add(new Tuple2<String, String>(paramT.split(",")[0], paramT.split(",")[1]));
					
					return context;
				}
	        });
	        
	        orgRDD.saveAsTextFile(args[1]);
	        
	        */
	        /*
	        System.out.println(args[0]);
	        JavaRDD<String> words = sc.textFile(args[0]).flatMap(new FlatMapFunction<String, String>(){
	        		
				public Iterable<String> call(String paramT) throws Exception {
					System.out.println(paramT);
					return Arrays.asList(paramT.split(","));
				}

	           
	        });
	        words.saveAsTextFile(args[1]);
	        */
	        
	        
	        
	        sc.stop();
	        
	    }
	    
	    static class MapResult extends AbstractFunction1<ResultSet, Object[]> implements Serializable {

	        public Object[] apply(ResultSet row) {
	            return JdbcRDD.resultSetToObjectArray(row);
	        }
	    }
}
