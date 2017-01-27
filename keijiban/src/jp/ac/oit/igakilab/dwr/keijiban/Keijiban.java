package jp.ac.oit.igakilab.dwr.keijiban;

import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

public class Keijiban {
	/**
	 * データベースに登録されている投稿の一覧を取得します
	 * @return 投稿の履歴(PostFormの配列)
	 */
	public List<PostForm> getMessages(String room){
		//TODO: 投稿取得メソッドの実装

		//ダミーデータを作成
		PostForm p1 = new PostForm();
		PostForm p2 = new PostForm();
		Calendar cal = Calendar.getInstance();
		p1.setName("ryo");
		p1.setMessage("いまどこー？");
		p2.setName("kita");
		p2.setMessage("二度寝した");

		p2.setTime(cal.getTime());
		cal.add(Calendar.MINUTE, -3);
		p1.setTime(cal.getTime());

		List<PostForm> dummy = Arrays.asList(p1, p2);

		//ダミーデータを返却
		return dummy;
	}

	/**
	 * メッセージを新しく投稿します
	 * @param post 送信者とメッセージのデータ(PostForm, timeは指定しなくてよい)
	 * @return　投稿に成功した時true
	 */
	public boolean postMessage(PostForm post){
		//TODO: 投稿登録メソッドの実装
		KeijibanDB db = new KeijibanDB();

        db.postMessage(post.getName(), post.getRoom(), post.getMessage());

        db.closeClient();
        return true;

	}

	/**
	 * 現在ある部屋（スレッド）のリストを取得します
	 * @return 部屋の名前の配列(Stringの配列)
	 */
	public List<String> getRoomList(){
		//TODO: 部屋リスト取得メソッドの実装
		return Arrays.asList("sample-room1", "sample-room2");
	}
}
