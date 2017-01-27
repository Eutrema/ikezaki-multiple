package jp.ac.oit.igakilab.dwr.keijiban;

import java.util.Arrays;
import java.util.Calendar;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;

/**
 * DB縺ｫ繧｢繧ｯ繧ｻ繧ｹ縺吶ｋ縺溘ａ縺ｮ繧ｯ繝ｩ繧ｹ縺ｧ縺�
 * client縺ｮ蛻晄悄蛹悶ｄ縲．B縺ｮ蜿門ｾ玲擅莉ｶ謖�螳壹ｄ繝�繝ｼ繧ｿ逋ｻ骭ｲ繧定｡後＞縺ｾ縺�
 * 繝�繝ｼ繧ｿ蜿門ｾ礼ｳｻ縺ｧ縺ｯDocument蝙九�ｮDB繧ｫ繝ｼ繧ｽ繝ｫ縺ｮ霑泌唆縲�
 * 繝�繝ｼ繧ｿ逋ｻ骭ｲ邉ｻ縺ｧ縺ｯ隍�謨ｰ縺ｮ繝代Λ繝｡繝ｼ繧ｿ縺九ｉDocument繧堤函謌舌＠DB縺ｫ逋ｻ骭ｲ縺吶ｋ縺ｾ縺ｧ繧貞�ｦ逅�縺励∪縺吶��
 * @author ryokun
 *
 */
public class KeijibanDB {
	//DB繧ｵ繝ｼ繝舌�ｼ縺ｮ險ｭ螳�
	static String DB_HOST = "localhost";
	static int DB_PORT = 27017;

	//DB縺ｮ繧ｳ繝ｬ繧ｯ繧ｷ繝ｧ繝ｳ險ｭ螳�
	static String DB_NAME = "keijiban";
	static String COL_NAME = "posts";


	private MongoClient client; //DB縺ｮ繧ｯ繝ｩ繧､繧｢繝ｳ繝医う繝ｳ繧ｹ繧ｿ繝ｳ繧ｹ

	//繧ｳ繝ｳ繧ｹ繝医Λ繧ｯ繧ｿ
	public KeijibanDB(){
		client = new MongoClient(DB_HOST, DB_PORT);
	}

	//繧ｳ繝ｬ繧ｯ繧ｷ繝ｧ繝ｳ縺ｮ蜿門ｾ�
	private MongoCollection<Document> getCollection(){
		return client.getDatabase(DB_NAME).getCollection(COL_NAME);
	}

	/**
	 * DB縺ｫ逋ｻ骭ｲ縺輔ｌ縺ｦ縺�繧脚oom縺ｧ縺ｮ逋ｺ險�繧貞叙蠕励＠縺ｾ縺�
	 * 蜿門ｾ玲凾縺ｫtime(謚慕ｨｿ譌･譎�)縺ｮ譁ｰ縺励＞鬆�縺ｫ荳ｦ縺ｳ譖ｿ縺医∪縺吶��
	 * @param room 驛ｨ螻九�ｮ蜷榊燕
	 * @return DB繧ｫ繝ｼ繧ｽ繝ｫ
	 */
    public FindIterable<Document> getMessages(String room){
        return getCollection()
            .find(Filters.eq("room", room))
            .sort(Sorts.ascending("time"));
    }

	/**
	 * 繝｡繝�繧ｻ繝ｼ繧ｸ繧呈眠縺励￥謚慕ｨｿ縺励∪縺�
	 * @param 驛ｨ螻九�ｮ蜷榊燕
	 * @param name 謚慕ｨｿ閠�蜷�
	 * @param message 譛ｬ譁�
	 */
	public void postMessage(String name, String room, String message){
		//TODO: DB縺ｸ縺ｮ謚慕ｨｿ讖溯�ｽ繧貞ｮ溯｣�
		Document doc = new Document("name", name)
	            .append("room", room)
	            .append("message", message)
	            .append("time", Calendar.getInstance().getTime());

	    getCollection().insertOne(doc);

	}

	/**
	 * DB縺ｫ逋ｻ骭ｲ縺輔ｌ縺ｦ縺�繧区兜遞ｿ縺ｫ縺九ｉ縲�驛ｨ螻九�ｮ繝ｪ繧ｹ繝医ｒ蜿門ｾ励＠縺ｾ縺�
	 * @return DB繧ｫ繝ｼ繧ｽ繝ｫ
	 */
	public AggregateIterable<Document> getRoomList(){
		//TODO: DB縺九ｉ縺ｮ驛ｨ螻九Μ繧ｹ繝亥叙蠕玲ｩ溯�ｽ繧貞ｮ溯｣�
		 return getCollection().aggregate(Arrays.asList(
		            Aggregates.group("$room")));
	}


	/**
	 * DB繧ｯ繝ｩ繧､繧｢繝ｳ繝医ｒ繧ｯ繝ｭ繝ｼ繧ｺ縺励∪縺�
	 */
	public void closeClient(){
		client.close();
	}



}
