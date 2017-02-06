package jp.ac.oit.igakilab.dwr.zaiko;

import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;

/**
 * DBにアクセスするためのクラスです
 * clientの初期化や、DBの取得条件指定やデータ登録を行います
 * データ取得系ではDocument型のDBカーソルの返却、
 * データ登録系では複数のパラメータからDocumentを生成しDBに登録するまでを処理します。
 * @author ryokun
 *
 */
public class ZaikoDB {
	//DBサーバーの設定
	static String DB_HOST = "localhost";
	static int DB_PORT = 27017;

	//DBのコレクションの設定
	static String DB_NAME = "zaiko";
	static String COL_NAME = "receipts";


	private MongoClient client; //DBのクライアントインスタンス

	//コンストラクタ
	public ZaikoDB(){
		client = new MongoClient(DB_HOST, DB_PORT);
	}

	//コレクションの取得
	private MongoCollection<Document> getCollection(){
		return client.getDatabase(DB_NAME).getCollection(COL_NAME);
	}

	//在庫の合計を計算するメソッド
	private int getItemQuantity(String itemName){
		List<Bson> query = Arrays.asList(
			Aggregates.match(Filters.eq("name", itemName)),
			Aggregates.group(null, Accumulators.sum("qty", "$amount")));

		Document doc = getCollection().aggregate(query).first();

		return doc != null ? doc.getInteger("qty", 0) : 0;
	}

	/**
	 * DBに登録されている商品の残りの在庫数を取得します
	 * @return DBカーソル
	 */
    public AggregateIterable<Document> getItemList(){
        //入荷/出荷データの合計を計算する
        return getCollection().aggregate(Arrays.asList(
            Aggregates.group("$name", Accumulators.sum("qty", "$amount"))));
    }

	/**
	 * 在庫DBに商品を追加します
	 * @param itemName 商品名
	 * @param amount 追加数
	 */
	public void receiveItem(String itemName, int amount){
        Document doc = new Document("name", itemName)
            .append("amount", amount)
            .append("time", Calendar.getInstance().getTime());

        getCollection().insertOne(doc);
    }
	/**
	 * 在庫DBから商品を出庫します
	 * もし、在庫が足りていない場合はfalseを返却し、出庫処理は行いません
	 * @param itemName 商品名
	 * @param amount 出庫(依頼)数
	 * @return 在庫があり、出庫処理が成功すればtrue
	 */
	public boolean issueItem(String itemName, int amount){
		//TODO: DBに出庫を記録する機能の実装

		return false;
	}

	/**
	 * 在庫DBから指定された商品の記録を取得します
	 * 返却されるデータは日付で昇順に整列されています。
	 * @param itemName 商品名
	 * @return DBカーソル
	 */
	public FindIterable<Document> getItemReceipts(String itemName){
		//TODO: DBから入出荷の履歴を取得する機能を実装

		return null;
	}

	/**
	 * DBをクローズします
	 */
	public void closeClient(){
		client.close();
	}
}
