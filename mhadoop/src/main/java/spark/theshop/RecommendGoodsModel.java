package spark.theshop;

import java.io.Serializable;

public class RecommendGoodsModel implements Serializable {
	private String id;
	private String userKey;
	private String goodsCd;
	private String goodsNm;
	private Double score;
	private String recommDt;
	private String pharmacyName;
	private String pharmacySido;
	private String pharmacyGugun;
	private String pharmacyDong;
	private String pharmacyAddr;
	
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
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getPharmacySido() {
		return pharmacySido;
	}
	public void setPharmacySido(String pharmacySido) {
		this.pharmacySido = pharmacySido;
	}
	public String getPharmacyGugun() {
		return pharmacyGugun;
	}
	public void setPharmacyGugun(String pharmacyGugun) {
		this.pharmacyGugun = pharmacyGugun;
	}
	public String getPharmacyDong() {
		return pharmacyDong;
	}
	public void setPharmacyDong(String pharmacyDong) {
		this.pharmacyDong = pharmacyDong;
	}
	public String getPharmacyAddr() {
		return pharmacyAddr;
	}
	public void setPharmacyAddr(String pharmacyAddr) {
		this.pharmacyAddr = pharmacyAddr;
	}
	
	
}
