package jp.ac.oit.igakilab.dwr.keijiban;

import java.util.Date;

/**
 * 掲示板への一つの投稿を表すクラスです
 * dwrでクライアントとやり取りするときに利用します
 * @author ryokun
 *
 */
public class PostForm {
	/**
	 * 投稿者の名前
	 */
	private String name;

	/**
	 * 投稿された部屋
	 */
	private String room;

	/**
	 * 投稿の本文
	 */
	private String message;

	/**
	 * 投稿日時
	 */
	private Date time;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getRoom() {
		return room;
	}

	public void setRoom(String room) {
		this.room = room;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public Date getTime() {
		return time;
	}

	public void setTime(Date time) {
		this.time = time;
	}
}
