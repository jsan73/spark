package spark.theshop;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import scala.Tuple2;


import spark.OrderInfoModel;

public class GoodsRank {
	
	static SimpleDateFormat iformatter = new SimpleDateFormat("yyyyMMddHHmmss");
	static SimpleDateFormat tformatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private static class Sum implements Function2<Double, Double, Double> {
		    public Double call(Double a, Double b) {
		      return a + b;
		    }
		  }
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

		JavaRDD<String> orders = sc.textFile(args[0], 1);
		
		JavaRDD<String> nonEmptyLines = orders.filter(new Function<String, Boolean>() {
		      public Boolean call(String s) throws Exception {
		        if(s == null || s.trim().length() < 1) {
		          return false;
		        }
		        return true;
		      }
		    });
		
		JavaRDD<OrderInfoModel> orderRDD = nonEmptyLines.map(new Function<String, OrderInfoModel>() {
			public OrderInfoModel call(String s) {
				String[] orderData = s.split("\\^");
				OrderInfoModel orderInfo = new OrderInfoModel();
				orderInfo.setOrderNum(orderData[0]);
				//"2015-11-04T08:11:02"
				Date date = null;
				try {
					//System.out.println(s);
					
					date = iformatter.parse(orderData[1]);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					System.out.println(s);
				}
				tformatter.setTimeZone(TimeZone.getTimeZone("UTC"));
				String sdate = tformatter.format(date).replace(" ", "T");
				
				orderInfo.setOrderDt(sdate);
				orderInfo.setUserKey(orderData[2]);
				orderInfo.setPayForm(orderData[3]);
				orderInfo.setGoodsNm(orderData[5]);
				orderInfo.setOrderQty(Integer.parseInt(orderData[6]));
				orderInfo.setSellerCd(orderData[7]);
				int price = (int)Double.parseDouble(orderData[8]);
				orderInfo.setGoodsPrice( price * Integer.parseInt(orderData[6]));
				orderInfo.setGoodsCd(orderData[9].toString());

				return orderInfo;		      
			}
		});
		orderRDD.persist(StorageLevel.MEMORY_AND_DISK());
		
		// 구매 횟수
	    JavaPairRDD<String, Double> goodsPairs = orderRDD.mapToPair(new PairFunction<OrderInfoModel, String, Double>() {
			public Tuple2<String, Double> call(OrderInfoModel arg0) throws Exception {
				return new Tuple2<String, Double>(arg0.getGoodsNm(), 1.0);
			}
	    });

	    JavaPairRDD<String, Double> orderCount = goodsPairs.reduceByKey(new Function2<Double, Double, Double>() {
	      public Double call(Double n1, Double n2) throws Exception {
	        return (n1 + n2) * 0.3;
	      }
	    });	    
	    
	    
		// 구매 유저수
	    JavaPairRDD<String, Iterable<String>> orderUser = orderRDD.mapToPair(new PairFunction<OrderInfoModel, String, String>(){
			public Tuple2<String, String> call(OrderInfoModel paramT)
					throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, String>(paramT.getGoodsNm(), paramT.getUserKey());
			}
		}).distinct().groupByKey().cache();
	    
    
	    JavaPairRDD<String, Double> orderUserCount = orderUser.mapValues(new Function<Iterable<String>, Double>() {
	      public Double call(Iterable<String> rs) {
	    	  List<String> finalList = new ArrayList<String>();
	    	  for (String s : rs) {
	    	      if (!finalList.contains(s)) {
	    	          finalList.add(s);
	    	      }
	    	  }
	    	  int size = finalList.size();
	        return size*1.5;
	      }
	    });

	    // 합치기
	    JavaPairRDD<String, Double> contribs = orderCount.union(orderUserCount);
	    
	    JavaPairRDD<String, Double> goodsRank = contribs.reduceByKey(new Function2<Double, Double, Double>() {
		      public Double call(Double n1, Double n2) throws Exception {
		        return n1 + n2;
		      }
	    });	    

	    
	    JavaRDD<GoodsRankModel> rankRDD = goodsRank.map(new Function<Tuple2<String, Double>, GoodsRankModel>() {

			public GoodsRankModel call(Tuple2<String, Double> arg0)
					throws Exception {
				GoodsRankModel rank = new GoodsRankModel();
				rank.setGoodsNm(arg0._1);
				rank.setRankScore(arg0._2);
				Date date = null;
				try {
			
					date = iformatter.parse("20151001010101");
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				tformatter.setTimeZone(TimeZone.getTimeZone("UTC"));
				String sdate = tformatter.format(date).replace(" ", "T");
				rank.setRankDt(sdate);
				return rank;
			}
	    	
	    });
	    
	    JavaEsSpark.saveToEs(rankRDD, "/theshop-goods-rank/rank");
	    
		sc.stop();
	}
}
