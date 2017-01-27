package jp.ac.oit.igakilab.dwr.zaiko;

import java.util.Date;

/**
 * 商品の名前と数量を表すクラスです
 * dwrでクライアントとやり取りするときに利用します
 * @author ryokun
 *
 */
public class ItemForm {
	/**
	 * 商品名
	 */
	private String name;

	/**
	 * 数量
	 */
	private int amount;

	/**
	 * 時刻
	 */
	private Date time;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getAmount() {
		return amount;
	}

	public void setAmount(int amount) {
		this.amount = amount;
	}

	public Date getTime() {
		return time;
	}

	public void setTime(Date time) {
		this.time = time;
	}
}
