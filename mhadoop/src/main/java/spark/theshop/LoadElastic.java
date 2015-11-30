package spark.theshop;

public class LoadElastic {
	// Loads all URLs from input file and initialize their neighbors.
	/*
    JavaPairRDD<String, Iterable<String>> links = orderRDD.mapToPair(new PairFunction<OrderInfoModel, String, String>(){
		public Tuple2<String, String> call(OrderInfoModel paramT)
				throws Exception {
			// TODO Auto-generated method stub
			return new Tuple2<String, String>(paramT.getUserKey(), paramT.getGoodsNm());
		}
	}).distinct().groupByKey().cache();
    
    

    // Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    JavaPairRDD<String, Double> ranks = links.mapValues(new Function<Iterable<String>, Double>() {
      public Double call(Iterable<String> rs) {
        return 1.0;
      }
    });

    // Calculates and updates URL ranks continuously using PageRank algorithm.
    for (int current = 0; current < 1; current++) {
      // Calculates URL contributions to the rank of other URLs.
      JavaPairRDD<String, Double> contribs = links.join(ranks).values()
        .flatMapToPair(new PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>() {
          public Iterable<Tuple2<String, Double>> call(Tuple2<Iterable<String>, Double> s) {
            int urlCount = Iterables.size(s._1);
            List<Tuple2<String, Double>> results = new ArrayList<Tuple2<String, Double>>();
            for (String n : s._1) {
              results.add(new Tuple2<String, Double>(n, s._2() / urlCount));
            }
            return results;
          }
      });

      // Re-calculates URL ranks based on neighbor contributions.
      ranks = contribs.reduceByKey(new Sum()).mapValues(new Function<Double, Double>() {
        public Double call(Double sum) {
          return 0.15 + sum * 0.85;
        }
      });
    }

    JavaEsSpark.saveToEs(orderRDD, "/theshop-goods/order");
    
    JavaRDD<GoodsRankModel> goodsRank = ranks.map(new Function<Tuple2<String, Double>, GoodsRankModel>() {

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
    
    JavaEsSpark.saveToEs(goodsRank, "/theshop-goods-rank/rank");
    */
    // Collects all URL ranks and dump them to console.
    /*
    List<Tuple2<String, Double>> output = ranks.collect();
    System.out.println("RESULT : ===================== : " + output.size());
    for (Tuple2<?,?> tuple : output) {
        System.out.println(tuple._1() + " has rank: " + tuple._2());
    }


	
	
	
	// 상품 정보 통계
	/*
	JavaPairRDD<String, OrderInfoModel> goodsPairRDD = orderRDD.mapToPair(new PairFunction<OrderInfoModel, String, OrderInfoModel>(){
		public Tuple2<String, OrderInfoModel> call(OrderInfoModel paramT)
				throws Exception {
			// TODO Auto-generated method stub
			return new Tuple2<String, OrderInfoModel>(paramT.getGoodsCd(), paramT);
		}
	});

	JavaPairRDD<String, OrderInfoModel> goodsCountRDD = goodsPairRDD.reduceByKey(new Function2<OrderInfoModel,OrderInfoModel,OrderInfoModel>() {
		public OrderInfoModel call(OrderInfoModel paramT1,
				OrderInfoModel paramT2) throws Exception {
			OrderInfoModel order = new OrderInfoModel();
			
			order.setGoodsPrice(paramT1.getGoodsPrice() + paramT2.getGoodsPrice());
			order.setOrderQty(paramT1.getOrderQty() + paramT2.getOrderQty());
			order.setGoodsNm(paramT1.getGoodsNm());
			return order;
		}
	});
	JavaEsSpark.saveToEsWithMeta(goodsCountRDD, "/theshop-goods/order");
	*/
	
    //List<OrderInfoModel> output = orderRDD.collect();
    
    //for (TuOrderInfoModel order : output) {
      //  System.out.println(order.getGoodsNm());
    //}
}
