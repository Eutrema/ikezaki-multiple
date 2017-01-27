package jp.ac.oit.igakilab.dwr.keijiban.test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import jp.ac.oit.igakilab.dwr.keijiban.Keijiban;
import jp.ac.oit.igakilab.dwr.keijiban.PostForm;

/**
 * Keijibanクラスをテストするクラスです
 * @author Ryokun
 *
 */
public class TestKeijiban {
	public static void main(String[] args){
		System.out.println("--- TEST getMessages");
		testGetMessages();
		System.out.println();

		System.out.println("--- TEST postMessage");
		testPostMessage();

		System.out.println("--- TEST getRoomList");
		testGetRoomList();
	}

	public static void testGetMessages(){
		String room = "test";
		Keijiban k = new Keijiban();
		DateFormat df = new SimpleDateFormat("yy/MM/dd HH:mm");

		for(PostForm p : k.getMessages(room)){
			System.out.format("%s : %s (%s)\n",
				p.getName(), p.getMessage(), df.format(p.getTime()));
		}
	}

	public static void testPostMessage(){
		String name = "ue";
		String msg = "今日学校行かなあかん？";
		Keijiban k = new Keijiban();

		System.out.format("[%s: %s]を投稿", name, msg);
		PostForm p = new PostForm();
		p.setRoom("test");
		p.setName(name);
		p.setMessage(msg);

		System.out.println("return: " + k.postMessage(p));
	}

	public static void testGetRoomList(){
		Keijiban k = new Keijiban();

		for(String r : k.getRoomList()){
			System.out.println(r);
		}
	}
}
