package cg.demo;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.ConcurrentNavigableMap;

import javax.swing.text.StyledEditorKit.ItalicAction;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

public class createMapDBDemo {

	private long num = 0;

	public void createTree() throws IOException {

		File dbFile = new File("atoiDB");
		File dbFile2 = new File("itoaDB");
		//FileReader srcFile = new FileReader("/dawnfs/users/lwh/Jar/lubm/lubm_700.nt");
		FileReader srcFile = new FileReader("University.nt");
		BufferedReader reader = new BufferedReader(srcFile);
		
		BufferedOutputStream out = new BufferedOutputStream(
			new FileOutputStream("University.txt"));

		DB db = DBMaker.newFileDB(dbFile)
				.closeOnJvmShutdown()
				.writeAheadLogDisable()
		//		.transactionDisable()
				.randomAccessFileEnable()
		//		.encryptionEnable("password")
				.make();

		DB db2 = DBMaker.newFileDB(dbFile2)
				.closeOnJvmShutdown()
				.writeAheadLogDisable()
		//		.transactionDisable()
				.randomAccessFileEnable()
		//		.encryptionEnable("password")
				.make();

		// open an collection, TreeMap has better performance then HashMap
		// ConcurrentNavigableMap<Integer, String> map = db
		// .getTreeMap("convertTree");

		HTreeMap<String, Long> aToiMap = db.getHashMap("HTree");
		//HTreeMap<Long, String> iToaMap = db2.getHashMap("hashTree");
		BTreeMap<Long, String> iToaMap = db2.getTreeMap("BTree");
		
		String line = null;
		String writeLine = "";
		String[] text;
		while ((line = reader.readLine()) != null) {
			text = line.split(" ");
			for (int i = 0; i < 3; i++) {

				Long ke = null;
				if ((ke = aToiMap.get(text[i])) == null) {
					aToiMap.put(text[i], num);
					iToaMap.put(num, text[i]);
					writeLine = writeLine + num + " ";
					num++;
				}else{
					writeLine = writeLine + ke + " ";
				}
				// System.out.println("adding:"+text[i]);

			}
			writeLine = writeLine + "\n";
			out.write(writeLine.getBytes());
			writeLine = "";
		}
		System.out.println("total1:" + num);
		System.out.println("total2:" + aToiMap.size());
		System.out.println("total3:" + iToaMap.size());
		// map.put(1, "one");
		// map.put(2, "two");
		// map.keySet() is now [1,2] even before commit

		db.commit(); // persist changes into disk
		db2.commit();
		// map.put(3, "three");
		// map.keySet() is now [1,2,3]
		// db.rollback(); // revert recent changes
		// map.keySet() is now [1,2]

		out.flush();
		out.close();
		
		reader.close();
		db.close();
		db2.close();
	}
	
	public void createAtoI() throws IOException {

		File dbFile = new File("atoiDB");
		
		FileReader srcFile = new FileReader("/dawnfs/users/lwh/Jar/lubm/lubm_700.nt");
		//FileReader srcFile = new FileReader("University.nt");
		BufferedReader reader = new BufferedReader(srcFile);
		
		BufferedOutputStream out = new BufferedOutputStream(
			new FileOutputStream("University.txt"));

		DB db = DBMaker.newFileDB(dbFile)
				.closeOnJvmShutdown()
				.writeAheadLogDisable()
		//		.transactionDisable()
				.randomAccessFileEnable()
		//		.encryptionEnable("password")
				.make();

		// open an collection, TreeMap has better performance then HashMap
		// ConcurrentNavigableMap<Integer, String> map = db
		// .getTreeMap("convertTree");

		HTreeMap<String, Long> aToiMap = db.getHashMap("HTree");
		//HTreeMap<Long, String> iToaMap = db2.getHashMap("hashTree");
		
		String line = null;
		String writeLine = "";
		String[] text;
		while ((line = reader.readLine()) != null) {
			text = line.split(" ");
			for (int i = 0; i < 3; i++) {

				Long ke = null;
				if ((ke = aToiMap.get(text[i])) == null) {
					aToiMap.put(text[i], num);
					writeLine = writeLine + num + " ";
					num++;
				}else{
					writeLine = writeLine + ke + " ";
				}
				// System.out.println("adding:"+text[i]);

			}
			writeLine = writeLine + "\n";
			out.write(writeLine.getBytes());
			writeLine = "";
		}
		System.out.println("total1:" + num);
		System.out.println("total2:" + aToiMap.size());

		db.commit(); // persist changes into disk

		out.flush();
		out.close();
		
		reader.close();
		db.close();
	}
	
	public void createItoA() throws IOException {

		File dbFile = new File("atoiDB");
		File dbFile2 = new File("itoaDB");
		
		DB db = DBMaker.newFileDB(dbFile)
				.closeOnJvmShutdown()
				.writeAheadLogDisable()
		//		.transactionDisable()
				.randomAccessFileEnable()
		//		.encryptionEnable("password")
				.make();

		DB db2 = DBMaker.newFileDB(dbFile2)
				.closeOnJvmShutdown()
				.writeAheadLogDisable()
		//		.transactionDisable()
				.randomAccessFileEnable()
		//		.encryptionEnable("password")
				.make();

		// open an collection, TreeMap has better performance then HashMap
		// ConcurrentNavigableMap<Integer, String> map = db
		// .getTreeMap("convertTree");

		HTreeMap<String, Long> aToiMap = db.getHashMap("HTree");
		//HTreeMap<Long, String> iToaMap = db2.getHashMap("hashTree");
		BTreeMap<Long, String> iToaMap = db2.getTreeMap("BTree");
		
		for(String str : aToiMap.keySet()){
			iToaMap.put(aToiMap.get(str), str);
		}
		
		System.out.println("total2:" + aToiMap.size());
		System.out.println("total3:" + iToaMap.size());

		db2.commit();

		db.close();
		db2.close();
	}

	public void searchTree() {
		
		File dbFile = new File("atoiDB");
		File dbFile2 = new File("itoaDB");
		
		DB db = DBMaker.newFileDB(dbFile)
				.closeOnJvmShutdown()
				.writeAheadLogDisable()
		//		.transactionDisable()
				.randomAccessFileEnable()
		//		.encryptionEnable("password")
				.make();

		DB db2 = DBMaker.newFileDB(dbFile2)
				.closeOnJvmShutdown()
				.writeAheadLogDisable()
		//		.transactionDisable()
				.randomAccessFileEnable()
		//		.encryptionEnable("password")
				.make();
		
		// open an collection, TreeMap has better performance then HashMap
		HTreeMap<String, Long> aToiMap = db.getHashMap("HTree");
		//HTreeMap<Long, String> iToaMap = db2.getHashMap("hashTree");
		BTreeMap<Long, String> iToaMap = db2.getTreeMap("BTree");

		Scanner sc = new Scanner(System.in);
		String line;
		System.out.print("find:");
		
		while ((line = sc.nextLine()) != null) {
			long id = Long.parseLong(line);
			String rs = iToaMap.get(id);
			System.out.println("result:" + rs);
			System.out.println("result2:" + aToiMap.get(rs));
			System.out.print("find:");
		}
		
		sc.close();
		db.close();
		db2.close();
	}

	public void searchTree2() throws IOException {
		
		File dbFile = new File("atoiDB");
		File dbFile2 = new File("itoaDB");
		
		DB db = DBMaker.newFileDB(dbFile)
				.closeOnJvmShutdown()
				.writeAheadLogDisable()
		//		.transactionDisable()
				.randomAccessFileEnable()
		//		.encryptionEnable("password")
				.make();

		DB db2 = DBMaker.newFileDB(dbFile2)
				.closeOnJvmShutdown()
				.writeAheadLogDisable()
		//		.transactionDisable()
				.randomAccessFileEnable()
		//		.encryptionEnable("password")
				.make();
		
		// open an collection, TreeMap has better performance then HashMap
		HTreeMap<String, Long> aToiMap = db.getHashMap("HTree");
		//HTreeMap<Long, String> iToaMap = db2.getHashMap("hashTree");
		BTreeMap<Long, String> iToaMap = db2.getTreeMap("BTree");

		FileReader srcFile = new FileReader("University.nt");
		BufferedReader reader = new BufferedReader(srcFile);
		
		String line = null;
		String[] text;
		long a = System.currentTimeMillis();
		while ((line = reader.readLine()) != null) {
			text = line.split(" ");
			for (int i = 0; i < 3; i++) {

				Long t1 = aToiMap.get(text[i]);
				if (t1 == null || !iToaMap.get(t1).equalsIgnoreCase(text[i])) {
					System.out.print("miss match:" + text[i] + "  id:" + t1);
				}

			}
		}
		a = System.currentTimeMillis() - a;
		System.out.print("time:" + a + "ms");
		reader.close();
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// Configure and open database using builder pattern.
		// All options are available with code auto-completion.

		createMapDBDemo t = new createMapDBDemo();
		 try {
			t.createTree();
			//t.searchTree2();
			 //t.createAtoI();
			 //t.createItoA();
		 } catch (IOException e) {
		 // TODO Auto-generated catch block
			 e.printStackTrace();
		 }
		//t.searchTree();
		
	}

}
