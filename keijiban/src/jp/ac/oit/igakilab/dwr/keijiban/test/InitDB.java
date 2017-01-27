package jp.ac.oit.igakilab.dwr.keijiban.test;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * 演習で使用するDBを一度dropし、テストデータを書き込みます
 * @author Ryokun
 *
 */
public class InitDB {
	public static void main(String args[]){
		MongoClient client = new MongoClient("localhost", 27017);
		MongoDatabase db = client.getDatabase("keijiban");

		//データベース初期化
		db.drop();
		System.out.println("初期化完了");

		//テストデータ登録
		MongoCollection<Document> col = db.getCollection("posts");
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.MINUTE, -8);

		List<Document> data = new ArrayList<Document>();
		data.add(new Document("name", "take")
			.append("room", "test")
			.append("message", "次の評価実験は?")
			.append("time", cal.getTime()));
		cal.add(Calendar.MINUTE, 2);
		data.add(new Document("name", "haya")
			.append("room", "test")
			.append("message", "明日の14時からだよ")
			.append("time", cal.getTime()));
		cal.add(Calendar.MINUTE, 2);
		data.add(new Document("name", "take")
			.append("room", "test")
			.append("message", "たぶん被験者は3人だったよね？")
			.append("time", cal.getTime()));
		cal.add(Calendar.MINUTE, 2);
		data.add(new Document("name", "haya")
			.append("room", "test")
			.append("message", "その通り、寝坊するなよ")
			.append("time", cal.getTime()));

		data.add(new Document("name", "mitsu")
			.append("room", "test2")
			.append("message", "ここは違う部屋だよ")
			.append("time", cal.getTime()));

		col.insertMany(data);
		System.out.println("登録完了");

		client.close();
	}
}