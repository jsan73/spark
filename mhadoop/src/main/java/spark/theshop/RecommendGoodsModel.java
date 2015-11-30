package spark.theshop;

import java.io.Serializable;

public class RecommendGoodsModel implements Serializable {
	private String userKey;
	private String goodsCd;
	private String goodsNm;
	private Double score;
	private String recommDt;
	private String pharmacyName;
	
	public String getUserKey() {
		return userKey;
	}
	public void setUserKey(String userKey) {
		this.userKey = userKey;
	}
	public String getGoodsCd() {
		return goodsCd;
	}
	public void setGoodsCd(String goodsCd) {
		this.goodsCd = goodsCd;
	}
	public String getGoodsNm() {
		return goodsNm;
	}
	public void setGoodsNm(String goodsNm) {
		this.goodsNm = goodsNm;
	}
	public Double getScore() {
		return score;
	}
	public void setScore(Double score) {
		this.score = score;
	}
	public String getRecommDt() {
		return recommDt;
	}
	public void setRecommDt(String recommDt) {
		this.recommDt = recommDt;
	}
	public String getPharmacyName() {
		return pharmacyName;
	}
	public void setPharmacyName(String pharmacyName) {
		this.pharmacyName = pharmacyName;
	}
	
	
}
