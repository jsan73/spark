package spark.theshop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.storage.StorageLevel;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import scala.Tuple2;
import spark.OrderInfoModel;


public class RecommGoods {
	
	static class SelectData implements Function<String, Boolean> {
		private String sDate;
		private String eDate;
		public SelectData(String s, String e) {this.sDate = s; this.eDate = e;}
		public Boolean call(String x) {
			String[] orderData = x.split("\\^");
			if(Long.parseLong(orderData[1]) >= Long.parseLong(sDate) && Long.parseLong(orderData[1]) <= Long.parseLong(eDate))
				return true;
			else return false;
		}
	}
	
	
	final static String HDFS_URL = "hdfs://elastic:9000";
	

	
	 // 메인
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
		String startDt = "20151001000000";
		String endDt = "20151031235959";

		JavaRDD<String> orders = sc.textFile(args[0], 1).filter(new SelectData(startDt, endDt));

		final Broadcast<Map<String, OrderInfoModel>> bPharmacy = sc.broadcast(loadPharmacyTable());
		JavaRDD<OrderInfoModel> orderRDD = orders.map(new Function<String, OrderInfoModel>() {
			public OrderInfoModel call(String s) {
				String[] orderData = s.split("\\^");
				OrderInfoModel orderInfo = new OrderInfoModel();
				orderInfo.setUserKey(orderData[2]);
				orderInfo.setOrderQty((int) Double.parseDouble(orderData[4]));
				orderInfo.setGoodsCd(orderData[7].toString());
				orderInfo.setGoodsNm(orderData[3]);
				return orderInfo;		      
			}
		});
		orderRDD.persist(StorageLevel.DISK_ONLY());

		
		
		// 구매 횟수 및 갯수에 따른 점수 합산(userkey||상품)
	    JavaPairRDD<String, String> goodsPairs = orderRDD.mapToPair(new PairFunction<OrderInfoModel, String, String>() {
			public Tuple2<String, String> call(OrderInfoModel arg0) throws Exception {
				return new Tuple2<String, String>(arg0.getGoodsCd(), arg0.getGoodsNm());
			}
	    }).distinct();
	    
	    List <Tuple2<String, String>> goodsList = goodsPairs.collect();
	    final Map<String, String> goodsMap = new HashMap<String, String>();
	    for(Tuple2<String, String> i : goodsList) goodsMap.put(i._1, i._2);
	    
		
		// 구매 횟수 및 갯수에 따른 점수 합산(userkey||상품)
	    JavaPairRDD<String, Double> userGoodsPairs = orderRDD.mapToPair(new PairFunction<OrderInfoModel, String, Double>() {
			public Tuple2<String, Double> call(OrderInfoModel arg0) throws Exception {
				return new Tuple2<String, Double>(arg0.getUserKey() + "^" + arg0.getGoodsCd(), 1.0 * arg0.getOrderQty());
			}
	    });

	    JavaPairRDD<String, Double> orderCount = userGoodsPairs.reduceByKey(new Function2<Double, Double, Double>() {
	      public Double call(Double n1, Double n2) throws Exception {
	        return (n1 + n2);
	      }
	    });
	    
	    orderCount.persist(StorageLevel.DISK_ONLY());
	    
	    // 추천 데이터 생성
	    JavaRDD<String> data = orderCount.map(new Function<Tuple2<String, Double>, String>() {

			public String call(Tuple2<String, Double> arg0) throws Exception {
				// TODO Auto-generated method stub
				return arg0._1() + "^" + arg0._2();
			}
	    	
	    });
	    
	    
	    // Rating Data 생성
	    JavaRDD<Rating> ratings = data.map(new Function<String, Rating>() {
	    	public Rating call(String s) {
	    		String[] sarray = s.split("\\^");
	    		if(sarray.length == 3)
	    			return new Rating(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[1]), (float) Double.parseDouble(sarray[2]));
	    		else return null;
	    	}
	    });
	    
	 // Build the recommendation model using ALS
	    int rank = 10;
	    int numIterations = 20;
	    MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(ratings), rank, numIterations, 0.01);

	    // Evaluate the model on rating data
	    JavaRDD<Tuple2<Object, Object>> userProducts = ratings.map(
	            new Function<Rating, Tuple2<Object, Object>>() {
	                public Tuple2<Object, Object> call(Rating r) {
	                    return new Tuple2<Object, Object>(r.user(), r.product());
	                }
	            }
	    );
	    JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD.fromJavaRDD(
	            model.predict(JavaRDD.toRDD(userProducts)).toJavaRDD().map(
	                    new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
	                        public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating r) {
	                            return new Tuple2<Tuple2<Integer, Integer>, Double>(
	                                    new Tuple2<Integer, Integer>(r.user(), r.product()), (double) r.rating());
	                        }
	                    }
	            ));

	    JavaRDD<RecommendGoodsModel> recommRDD = predictions.map(new Function<Tuple2<Tuple2<Integer, Integer>, Double>, RecommendGoodsModel>() {

			public RecommendGoodsModel call(Tuple2<Tuple2<Integer, Integer>, Double> v1) throws Exception {
				// TODO Auto-generated method stub
				RecommendGoodsModel model = new RecommendGoodsModel();
				model.setUserKey(v1._1()._1().toString());
				model.setGoodsCd(v1._1()._2().toString());
				model.setGoodsNm(goodsMap.get(v1._1()._2().toString()));
				model.setScore(v1._2());
				model.setRecommDt("2015-10-01T12:00:00");
				OrderInfoModel pharmacyInfo = lookupPharmacy(v1._1()._1().toString(), bPharmacy.value());
				if(pharmacyInfo != null) {
					model.setPharmacyName(pharmacyInfo.getPharmacyName() + "(" + v1._1()._1().toString() + ")");
				}				
				return model;
			}
	    	
	    }); 
	    	
	    //JavaEsSpark.saveToEs(recommRDD, "/theshop-recommend/goods");
	    
	    
	    JavaPairRDD<String,Iterable<Map<String, String>>> recommPairRDD = recommRDD.mapToPair(new PairFunction<RecommendGoodsModel, String, Map<String, String>>(){
			public Tuple2<String, Map<String, String>> call(RecommendGoodsModel arg0) throws Exception {
				return new Tuple2<String, Map<String, String>>(arg0.getUserKey(),ImmutableMap.of("goodsCd", arg0.getGoodsCd(), "goodsNm", arg0.getGoodsNm()));
			}
	    }).groupByKey(); 
	    
	    JavaRDD<RecommendSaveModel> recommSaveRDD =  recommPairRDD.map(new Function<Tuple2<String, Iterable<Map<String, String>>>, RecommendSaveModel>() {

			public RecommendSaveModel call(Tuple2<String, Iterable<Map<String, String>>> arg0) throws Exception {
				// TODO Auto-generated method stub
				RecommendSaveModel save_goods = new RecommendSaveModel();
				
				save_goods.setGoodsList(arg0._2());
				save_goods.setUserKey(arg0._1());
				save_goods.setRecommDt("2015-10-01T12:00:00");
				return save_goods;
			}
	    });
	    JavaEsSpark.saveToEs(recommSaveRDD, "/theshop-recommend-user/goods",ImmutableMap.of("es.mapping.id", "userKey") );
	    
	    //<<Integer,Integer>,Double> to <Integer,<Integer,Double>>
	    /* 
	    JavaPairRDD<Integer, Tuple2<String, Double>> userPredictions = JavaPairRDD.fromJavaRDD(predictions.map(
	            new Function<Tuple2<Tuple2<Integer, Integer>, Double>, Tuple2<Integer, Tuple2<String, Double>>>() {
	                public Tuple2<Integer, Tuple2<String, Double>> call(Tuple2<Tuple2<Integer, Integer>, Double> v1) throws Exception {
	                	System.out.println(goodsMap.get(v1._1()._2().toString()));
	                    return new Tuple2<Integer, Tuple2<String, Double>>(v1._1()._1(), new Tuple2<String, Double>(goodsMap.get(v1._1()._2().toString()), v1._2()));
	                }
	            }
	    ));
	     
	    //Sort by key & Save
	    //userPredictions.sortByKey(true).saveAsTextFile("/home/hadoop/recommend.txt");
	  
	    JavaPairRDD<IntWritable, MyWritable> userPredictions = JavaPairRDD.fromJavaRDD(predictions.map(
	            new Function<Tuple2<Tuple2<Integer, Integer>, Double>, Tuple2<IntWritable, MyWritable>>() {
	                public Tuple2<IntWritable, MyWritable> call(Tuple2<Tuple2<Integer, Integer>, Double> v1) throws Exception {
	                    return new Tuple2<IntWritable, MyWritable>(new IntWritable(v1._1()._1()), new MyWritable(new Tuple2<String, Double>(goodsMap.get(v1._1()._2().toString()), v1._2())));
	                }
	            }
	    ));
	    */
	    //userPredictions.saveAsNewAPIHadoopFile(HDFS_URL + "/spark/theshop/recommend/201510", IntWritable.class,  MyWritable.class, SequenceFileOutputFormat.class);
	    
	    
		sc.stop();
	}
	
	static Map<String, OrderInfoModel> loadPharmacyTable() throws IOException, URISyntaxException {
		String pharmacyInfoFile = "/spark/theshop/info/pharmacy_info.txt";
		Map<String, OrderInfoModel> pharmacyMap = new HashMap<String, OrderInfoModel>();		
		Path pt=new Path(HDFS_URL + pharmacyInfoFile);
        FileSystem fs = FileSystem.get(new URI(HDFS_URL + pharmacyInfoFile), new Configuration());
        
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line;
        line=br.readLine();
        while (line != null){
			String[] pharmacyInfo = line.split("\\^");
			OrderInfoModel orderInfo = new OrderInfoModel();

			orderInfo.setPharmacyName(pharmacyInfo[1]);
			orderInfo.setPharmacyAddr(pharmacyInfo[2]);
			orderInfo.setPharmacySido(pharmacyInfo[3]);
			orderInfo.setPharmacyGugun(pharmacyInfo[4]);        	
			if(pharmacyInfo.length == 6)  orderInfo.setPharmacyDong(pharmacyInfo[5]);        	
        	pharmacyMap.put(pharmacyInfo[0], orderInfo);
        	
			line = br.readLine();
        }
        return pharmacyMap;
	}
	
	static OrderInfoModel lookupPharmacy(String userKey, Map<String, OrderInfoModel> pharmacyMap) {
		
		return pharmacyMap.get(userKey);
	}
	
}
