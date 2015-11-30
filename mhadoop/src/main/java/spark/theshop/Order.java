package spark.theshop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import spark.OrderInfoModel;

public class Order {
	
	
	final static String HDFS_URL = "hdfs://elastic:9000";

	 // ∏ﬁ¿Œ
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
	

		final Broadcast<Map<String, String>> bSeller = sc.broadcast(loadSellerTable());
		final Broadcast<Map<String, OrderInfoModel>> bPharmacy = sc.broadcast(loadPharmacyTable());

		
		JavaRDD<OrderInfoModel> orderRDD = orders.map(new Function<String, OrderInfoModel>() {
			public OrderInfoModel call(String s) {
				String[] orderData = s.split("\\^");
				OrderInfoModel orderInfo = new OrderInfoModel();
				orderInfo.setOrderNum(orderData[0]);
				//"2015-11-04T08:11:02"
				Date date = null;
				try {
					//System.out.println(orderData[1]);
					SimpleDateFormat iformatter = new SimpleDateFormat("yyyyMMddHHmmss");
					SimpleDateFormat tformatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					
					date = iformatter.parse(orderData[1]);
					tformatter.setTimeZone(TimeZone.getTimeZone("UTC"));
					String sdate = tformatter.format(date).replace(" ", "T");
					orderInfo.setOrderDt(sdate);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					System.out.println("ERROR : " + orderData[1] + " - " + e.getMessage());
					
				}
				
				
				
				OrderInfoModel pharmacyInfo = lookupPharmacy(orderData[2], bPharmacy.value());
				if(pharmacyInfo != null) {
					orderInfo.setPharmacyName(pharmacyInfo.getPharmacyName() + "(" + orderData[2] + ")");
					orderInfo.setPharmacyAddr(pharmacyInfo.getPharmacyAddr());
					orderInfo.setPharmacySido(pharmacyInfo.getPharmacySido());
					orderInfo.setPharmacyGugun(pharmacyInfo.getPharmacyGugun());        	
					orderInfo.setPharmacyDong(pharmacyInfo.getPharmacyDong());        	
				}
				orderInfo._id = orderData[0] + "|" + orderData[7];
				orderInfo.setUserKey(orderData[2]);
//				orderInfo.setPayForm(orderData[3]);
				orderInfo.setGoodsNm(orderData[3]);
				orderInfo.setOrderQty((int) Double.parseDouble(orderData[4]));
				orderInfo.setSellerCd(orderData[5]);
				orderInfo.setSellerName(lookupSeller(orderData[5], bSeller.value()));
				
				int price = (int)Double.parseDouble(orderData[6]);
				orderInfo.setGoodsPrice( price * (int) Double.parseDouble(orderData[4]));
				orderInfo.setGoodsCd(orderData[7].toString());
				

				return orderInfo;		      
			}
		});
		orderRDD.persist(StorageLevel.DISK_ONLY());
		JavaEsSpark.saveToEs(orderRDD, "/theshop-goods/order");
		
		
		sc.stop();
	}
	
	static Map<String, String> loadSellerTable() throws IOException, URISyntaxException {
		String sellerInfoFile = "/spark/theshop/info/seller_info.txt";
		Map<String, String> sellerMap = new HashMap<String, String>();		
		Path pt=new Path(HDFS_URL + sellerInfoFile);
        FileSystem fs = FileSystem.get(new URI(HDFS_URL + sellerInfoFile), new Configuration());
        
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line;
        line=br.readLine();
        while (line != null){
        	String[] sellerInfo = line.split("\\^");
			sellerMap.put(sellerInfo[0], sellerInfo[1]);
			line = br.readLine();
        }
        return sellerMap;
	}
	
	static String lookupSeller(String sellerCd, Map<String, String> sellerMap) {
		
		return sellerMap.get(sellerCd);
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
