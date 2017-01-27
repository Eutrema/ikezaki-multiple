package jp.ac.oit.igakilab.dwr.zaiko.test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import jp.ac.oit.igakilab.dwr.zaiko.ItemForm;
import jp.ac.oit.igakilab.dwr.zaiko.Zaiko;

public class TestZaiko {
	public static void main(String[] args){
		System.out.println("--- TEST receiveItem");
		testReceiveItem();
		System.out.println();

		System.out.println("--- TEST issueItem");
		testIssueItem();
		System.out.println();

		System.out.println("--- TEST getItemList");
		testGetItemList();
		System.out.println();

		System.out.println("--- TEST getItemHistory");
		testGetItemHistory();
		System.out.println();
	}

	public static void testGetItemList(){
		Zaiko z = new Zaiko();

		for(ItemForm i : z.getItemList()){
			System.out.format("%s: %d個\n", i.getName(), i.getAmount());
		}
	}

	public static void testReceiveItem(){
		String name = "オレオ";
		int amount = 5;
		Zaiko z = new Zaiko();

		System.out.format("%s を %d 個 入荷します\n", name, amount);
		ItemForm i = new ItemForm();
		i.setName(name);
		i.setAmount(amount);

		System.out.println("return: " + z.receiveItem(i));
	}

	public static void testIssueItem(){
		String name = "オレオ";
		int amount = 2;
		Zaiko z = new Zaiko();

		System.out.format("%s を %d 個 出荷します\n", name, amount);
		ItemForm i = new ItemForm();
		i.setName(name);
		i.setAmount(amount);

		System.out.println("return: " + z.issueItem(i));
	}

	public static void testGetItemHistory(){
		String name = "オレオ";
		Zaiko z = new Zaiko();
		DateFormat df = new SimpleDateFormat("yy/MM/dd HH:mm");

		System.out.format("%s の履歴\n", name);
		for(ItemForm i : z.getItemHistory(name)){
			System.out.format("%s %d (%s)\n",
				(i.getAmount() < 0 ? "出荷" : "入荷"),
				Math.abs(i.getAmount()),
				df.format(i.getTime()));
		}
	}
}
