package spark.theshop;

import java.io.Serializable;
import java.util.Map;

public class RecommendSaveModel implements Serializable {
	private String userKey;
	private Iterable<Map<String, String>> goodsList;
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
	public String getPharmacyName() {
		return pharmacyName;
	}
	public void setPharmacyName(String pharmacyName) {
		this.pharmacyName = pharmacyName;
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
