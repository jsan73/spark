package spark.theshop;

import java.io.Serializable;
import java.util.Map;

public class RecommendSaveModel implements Serializable {
	private String userKey;
	private Iterable<Map<String, String>> goodsList;
	private String recommDt;

	
	public String getUserKey() {
		return userKey;
	}
	public void setUserKey(String userKey) {
		this.userKey = userKey;
	}

	public String getRecommDt() {
		return recommDt;
	}
	public void setRecommDt(String recommDt) {
		this.recommDt = recommDt;
	}
	public Iterable<Map<String, String>> getGoodsList() {
		return goodsList;
	}
	public void setGoodsList(Iterable<Map<String, String>> goodsList) {
		this.goodsList = goodsList;
	}
	
}
