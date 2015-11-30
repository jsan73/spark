package spark.theshop;

import java.io.Serializable;

public class GoodsRankModel implements Serializable {
	private String goodsNm;
	private double rankScore;
	private String rankDt;
	public String getGoodsNm() {
		return goodsNm;
	}
	public void setGoodsNm(String goodsNm) {
		this.goodsNm = goodsNm;
	}
	public double getRankScore() {
		return rankScore;
	}
	public void setRankScore(double rankScore) {
		this.rankScore = rankScore;
	}
	public String getRankDt() {
		return rankDt;
	}
	public void setRankDt(String rankDate) {
		this.rankDt = rankDate;
	}
	
}
