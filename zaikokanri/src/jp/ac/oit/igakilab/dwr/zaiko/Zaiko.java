package jp.ac.oit.igakilab.dwr.zaiko;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class Zaiko {
	/**
	 * 在庫DBに登録されている商品とその商品の在庫数のリストを取得します
	 * ItemFormの値にはnameとamountのみ値を設定します
	 * @return 商品と在庫数のリスト(ItemFormの配列)
	 */
	public List<ItemForm> getItemList(){
		//TODO: 在庫リスト取得メソッドの実装

		//ダミーデータを作成
		List<ItemForm> dummy = new ArrayList<ItemForm>();
		Calendar cal = Calendar.getInstance();
		ItemForm i;

		cal.add(Calendar.MINUTE, -4);
		i = new ItemForm();
		i.setName("コーラ");
		i.setAmount(30);
		i.setTime(cal.getTime());
		dummy.add(i);

		cal.add(Calendar.MINUTE, 2);
		i = new ItemForm();
		i.setName("ポテトチップス");
		i.setAmount(10);
		i.setTime(cal.getTime());
		dummy.add(i);

		return dummy;
	}

	/**
	 * 在庫DBに商品を追加します
	 * @param recv 商品名と追加数量のデータ(ItemForm)
	 * @return 成功したときtrue
	 */
	public boolean receiveItem(ItemForm recv){
		//TODO: 入庫メソッドの実装

		return true;
	}

	/**
	 * 在庫DBからしていた数量の商品を出庫します
	 * @param req 商品名と出庫数量のデータ(ItemForm)
	 * @return 成功した時true, 在庫数が足りないとき、失敗した時false
	 */
	public boolean issueItem(ItemForm req){
		//TODO: 出庫メソッドの実装

		return true;
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
