package jp.ac.oit.igakilab.dwr.zaiko.test;

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
		MongoDatabase db = client.getDatabase("zaiko");

		//データベース初期化
		db.drop();
		System.out.println("初期化完了");

		//テストデータ登録
		MongoCollection<Document> col = db.getCollection("receipts");
		Calendar cal = Calendar.getInstance();

		List<Document> data = new ArrayList<Document>();
		cal.add(Calendar.MINUTE, -8);

		data.add(new Document("name", "コーラ")
			.append("amount", 24)
			.append("time", cal.getTime()));
		cal.add(Calendar.MINUTE, 2);
		data.add(new Document("name", "ラムネ")
			.append("amount", 36)
			.append("time", cal.getTime()));
		cal.add(Calendar.MINUTE, 2);
		data.add(new Document("name", "ポテトチップス")
			.append("amount", 12)
			.append("time", cal.getTime()));
		cal.add(Calendar.MINUTE, 2);
		data.add(new Document("name", "コーラ")
			.append("amount", -6)
			.append("time", cal.getTime()));

		col.insertMany(data);
		System.out.println("登録完了");

		client.close();
	}
}
