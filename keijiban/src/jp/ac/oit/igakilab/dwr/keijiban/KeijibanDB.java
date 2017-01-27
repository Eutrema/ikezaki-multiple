package jp.ac.oit.igakilab.dwr.keijiban;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;

/**
 * DBにアクセスするためのクラスです
 * clientの初期化や、DBの取得条件指定やデータ登録を行います
 * データ取得系ではDocument型のDBカーソルの返却、
 * データ登録系では複数のパラメータからDocumentを生成しDBに登録するまでを処理します。
 * @author ryokun
 *
 */
public class KeijibanDB {
	//DBサーバーの設定
	static String DB_HOST = "localhost";
	static int DB_PORT = 27017;

	//DBのコレクション設定
	static String DB_NAME = "keijiban";
	static String COL_NAME = "posts";


	private MongoClient client; //DBのクライアントインスタンス

	//コンストラクタ
	public KeijibanDB(){
		client = new MongoClient(DB_HOST, DB_PORT);
	}

	//コレクションの取得
	private MongoCollection<Document> getCollection(){
		return client.getDatabase(DB_NAME).getCollection(COL_NAME);
	}

	/**
	 * DBに登録されているroomでの発言を取得します
	 * 取得時にtime(投稿日時)の新しい順に並び替えます。
	 * @param room 部屋の名前
	 * @return DBカーソル
	 */
    public FindIterable<Document> getMessages(String room){
        return getCollection()
            .find(Filters.eq("room", room))
            .sort(Sorts.ascending("time"));
    }

	/**
	 * メッセージを新しく投稿します
	 * @param 部屋の名前
	 * @param name 投稿者名
	 * @param message 本文
	 */
	public void postMessage(String name, String room, String message){
		//TODO: DBへの投稿機能を実装

	}

	/**
	 * DBに登録されている投稿にから、部屋のリストを取得します
	 * @return DBカーソル
	 */
	public AggregateIterable<Document> getRoomList(){
		//TODO: DBからの部屋リスト取得機能を実装

		return null;
	}


	/**
	 * DBクライアントをクローズします
	 */
	public void closeClient(){
		client.close();
	}
}
