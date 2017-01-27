package jp.ac.oit.igakilab.dwr.keijiban;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bson.Document;

public class Keijiban {
	/**
	 * データベースに登録されている投稿の一覧を取得します
	 * @return 投稿の履歴(PostFormの配列)
	 */
    public List<PostForm> getMessages(String room){
        KeijibanDB db = new KeijibanDB();

        List<PostForm> messages = new ArrayList<PostForm>();
        for(Document doc : db.getMessages(room)){
            PostForm post = new PostForm();
            post.setName(doc.getString("name"));
            post.setMessage(doc.getString("message"));
            post.setTime(doc.getDate("time"));

            messages.add(post);
        }

        db.closeClient();
        return messages;
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
