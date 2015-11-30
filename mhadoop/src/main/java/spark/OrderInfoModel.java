package spark;

import java.io.Serializable;

public class OrderInfoModel implements Serializable {
	public String _id;
	private String orderNum;
	private String orderDt;
	private String userKey;
	private String payForm;
	private String goodsNm;
	private int orderQty;
	private int goodsPrice;
	private String goodsCd;
	private String sellerCd;
	private String sellerName;
	private String pharmacyName;
	private String pharmacySido;
	private String pharmacyGugun;
	private String pharmacyDong;
	private String pharmacyAddr;
	
	
	public String getSellerCd() {
		return sellerCd;
	}
	public void setSellerCd(String sellerCd) {
		this.sellerCd = sellerCd;
	}
	public String getOrderDt() {
		return orderDt;
	}
	public void setOrderDt(String orderDt) {
		this.orderDt = orderDt;
	}
	public String getUserKey() {
		return userKey;
	}
	public void setUserKey(String userKey) {
		this.userKey = userKey;
	}
	public String getPayForm() {
		return payForm;
	}
	public void setPayForm(String payForm) {
		this.payForm = payForm;
	}
	
	public String getGoodsCd() {
		return goodsCd;
	}
	public void setGoodsCd(String goodsCd) {
		this.goodsCd = goodsCd;
	}
	public String getOrderNum() {
		return orderNum;
	}
	public void setOrderNum(String orderNum) {
		this.orderNum = orderNum;
	}
	public String getGoodsNm() {
		return goodsNm;
	}
	public void setGoodsNm(String goodsNm) {
		this.goodsNm = goodsNm;
	}
	public int getOrderQty() {
		return orderQty;
	}
	public void setOrderQty(int orderQty) {
		this.orderQty = orderQty;
	}
	public int getGoodsPrice() {
		return goodsPrice;
	}
	public void setGoodsPrice(int goodsPrice) {
		this.goodsPrice = goodsPrice;
	}
	public String getSellerName() {
		return sellerName;
	}
	public void setSellerName(String sellerName) {
		this.sellerName = sellerName;
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
