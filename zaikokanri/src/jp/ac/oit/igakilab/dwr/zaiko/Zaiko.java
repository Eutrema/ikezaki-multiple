package jp.ac.oit.igakilab.dwr.zaiko;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.bson.Document;

public class Zaiko {
	/**
	 * 在庫DBに登録されている商品とその商品の在庫数のリストを取得します
	 * ItemFormの値にはnameとamountのみ値を設定します
	 * @return 商品と在庫数のリスト(ItemFormの配列)
	 */
	 public List<ItemForm> getItemList(){
        ZaikoDB db = new ZaikoDB();

        List<ItemForm> items = new ArrayList<ItemForm>();

        for(Document doc : db.getItemList()){
            ItemForm tmp = new ItemForm();
            tmp.setName(doc.getString("_id"));
            tmp.setAmount(doc.getInteger("qty", 0));

            items.add(tmp);
        }

        db.closeClient();
        return items;

    }

	/**
	 * 在庫DBに商品を追加します
	 * @param recv 商品名と追加数量のデータ(ItemForm)
	 * @return 成功したときtrue
	 */
	public boolean receiveItem(ItemForm recv){
        ZaikoDB db = new ZaikoDB();

        db.receiveItem(recv.getName(), recv.getAmount());

        db.closeClient();
        return true;
    }
	/**
	 * 在庫DBからしていた数量の商品を出庫します
	 * @param req 商品名と出庫数量のデータ(ItemForm)
	 * @return 成功した時true, 在庫数が足りないとき、失敗した時false
	 */
    public boolean issueItem(ItemForm req){
        ZaikoDB db = new ZaikoDB();

        boolean res = db.issueItem(req.getName(), req.getAmount());

        db.closeClient();
        return res;
    }

	/**
	 * 商品の出入の履歴を取得します
	 * @param itemName 商品名
	 * @return 出入庫の履歴
	 *
	 */
	public List<ItemForm> getItemHistory(String itemName){
		//TODO: 入出荷履歴取得メソッドの実装

		//ダミーデータを作成
		List<ItemForm> dummy = new ArrayList<ItemForm>();
		Calendar cal = Calendar.getInstance();
		ItemForm i;

		cal.add(Calendar.MINUTE, -4);
		i = new ItemForm();
		i.setName("ジンジャエール");
		i.setAmount(30);
		i.setTime(cal.getTime());
		dummy.add(i);

		cal.add(Calendar.MINUTE, 2);
		i = new ItemForm();
		i.setName("ジンジャエール");
		i.setAmount(-10);
		i.setTime(cal.getTime());
		dummy.add(i);

		return dummy;
	}

}
