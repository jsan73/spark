package spark.db;

import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.JdbcRDD;
import org.apache.spark.storage.StorageLevel;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import scala.Tuple2;
import scala.reflect.ClassManifestFactory$;
import spark.OrderInfoModel;
import spark.db.SaveObject.MapResult;

public class SendES {
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
	        JdbcRDD<Object[]> jdbcRDD = new JdbcRDD<Object[]>(sc.sc(), dbConnection, sql, 1, 1, 1, new MapResult(), ClassManifestFactory$.MODULE$.fromClass(Object[].class));

	        // Convert to JavaRDD
	        JavaRDD<Object[]> javaRDD = JavaRDD.fromRDD(jdbcRDD, ClassManifestFactory$.MODULE$.fromClass(Object[].class));

	        // Key-Value 조합
	        JavaPairRDD<String, OrderInfoModel> orders = javaRDD.mapToPair(new PairFunction<Object[], String, OrderInfoModel>() {
	            
				public Tuple2<String, OrderInfoModel> call(Object[] record)
						throws Exception {
					// TODO Auto-generated method stub
					OrderInfoModel order = new OrderInfoModel();
					order.setOrderNum(record[0].toString());
					order.setGoodsNm(record[5].toString());
					order.setOrderQty(Integer.parseInt(record[6].toString()));
					order.setGoodsPrice(Integer.parseInt(record[8].toString()));
					order.setGoodsCd(record[9].toString());
					//"2015-11-04T08:11:02"
					return new Tuple2<String, OrderInfoModel>(record[0].toString(), order);
				}
	        });
	        
	     // Key-Value 조합
	        JavaPairRDD<String, OrderInfoModel> orderGoods = orders.values().mapToPair(new PairFunction<OrderInfoModel, String, OrderInfoModel>() {

				public Tuple2<String, OrderInfoModel> call(OrderInfoModel arg0)
						throws Exception {
					// TODO Auto-generated method stub
					return new Tuple2<String, OrderInfoModel>(arg0.getOrderNum() + "|" + arg0.getGoodsCd(), arg0);
				}
	        });
  
	      //  JavaEsSpark.saveToEsWithMeta(orderGoods, "theshop/order");
	        
	        JavaPairRDD<String, Iterable<OrderInfoModel>> orderGroupBy = orders.groupByKey();
	        
	        // 주문 금액 합계
	        JavaPairRDD<String, Long> goodsPricePair = orderGroupBy.mapValues(new Function<Iterable<OrderInfoModel>, Long>() {

				public Long call(Iterable<OrderInfoModel> orderInfo) throws Exception {
					// TODO Auto-generated method stub
		            
					Iterator<OrderInfoModel> orderList = orderInfo.iterator();
					long goodsPrice = 0;
					while(orderList.hasNext()) {
						OrderInfoModel oinfo = orderList.next();
						System.out.println(oinfo.getGoodsPrice());
						goodsPrice += (oinfo.getGoodsPrice() * oinfo.getOrderQty());
					}
					return goodsPrice;
				}
	        	
	        });
	        goodsPricePair.persist(StorageLevel.MEMORY_ONLY());
	        
	        // 상품 판매 수량
	        
	      //  orderGoods.join(goodsPricePair);
	        
	        List<Tuple2<String, Long>> output = goodsPricePair.collect();
	        System.out.println("RESILT : ===================== : " + output.size());
	        for (Tuple2<?,?> tuple : output) {
	            System.out.println(tuple._1() + " has price: " + tuple._2() + ".");
	        }
	        
	        JavaEsSpark.saveToEsWithMeta(goodsPricePair, "theshop-order/orderPrice");

	        sc.stop();
	    }
	    
	    
	    /*
	    class ComputeGradient extends Function<DataPoint, Vector> {
	    	  private Vector w;
	    	  ComputeGradient(Vector w) { this.w = w; }
	    	  public Vector call(DataPoint p) {
	    	    return p.x.times(p.y * (1 / (1 + Math.exp(w.dot(p.x))) - 1));
	    	  }
	    	}

	    	JavaRDD<DataPoint> points = spark.textFile(...).map(new ParsePoint()).cache();
	    	Vector w = Vector.random(D); // current separating plane
	    	for (int i = 0; i < ITERATIONS; i++) {
	    	  Vector gradient = points.map(new ComputeGradient(w)).reduce(new AddVectors());
	    	  w = w.subtract(gradient);
	    	}
	    	System.out.println("Final separating plane: " + w);
	    	
	    	*/
}
