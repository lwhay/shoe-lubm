/**
 * 
 */
package cg.demo;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.regex.Pattern;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import cg.demo.CreateTrieDemo.Configuration;
import cg.demo.CreateTrieDemo.Replacer;
import cg.demo.CreateTrieDemo.ReplacerEntry;
import cg.trie.KeyAnalyzer;
import cg.trie.Trie;

/**
 * @author C_G
 * 
 */
public class CreateBPTreeDemo {
	private int size;
	private Pattern[] patterns;
	private Trie[] tries;
	private Replacer[] replacers;

	// wai，B+树索引
	public File dbFile;
	public DB db;
//	 open an collection, TreeMap has better performance then HashMap
	public BTreeMap<Long, String> bTreeMap;
	
	
//	public DB adb;
//	public DB idb;
//	public HTreeMap<String, Long> aToiMap;
//	//HTreeMap<Long, String> iToaMap = db2.getHashMap("hashTree");
//	public BTreeMap<Long, String> iToaMap;
//	//public HTreeMap<Long, String> bTreeMap;
	public CreateBPTreeDemo(String confPath) throws IOException {

		List<Configuration> confs = new ArrayList<Configuration>();

		initConfig(confs, confPath);
		initTrie(confs);
	//	initBPTree();
	}

	public void initConfig(List<Configuration> confs, String confPath)
			throws IOException {

		BufferedReader pread = new BufferedReader(new FileReader(confPath));
		String li = "";
		int thread = 0;
		while ((li = pread.readLine()) != null) {
			Configuration conf = new Configuration();

			String splits[] = li.trim().split("\\$");
			System.out.println(splits[0]);
			conf.setPattern(Pattern.compile(splits[0]));
			conf.setFile(splits[2]);
			Replacer replacer = new Replacer();
			String sps[] = splits[1].split("\\|");
			for (String s : sps) {
				String ss[] = s.split(",");
				if (ss.length == 2)
					replacer.put(ss[0], ss[1]);
				else if (ss.length == 1)
					replacer.put(ss[0], "");
			}
			conf.setReplacer(replacer);
			conf.setThread("Thread#" + thread);
			thread++;
			confs.add(conf);
		}
		pread.close();
	}

	public void initTrie(List<Configuration> confs) {
		size = confs.size();
		// System.out.println(size);
		patterns = new Pattern[size];
		tries = new Trie[size + 1];
		replacers = new Replacer[size];
		Iterator<Configuration> it = confs.iterator();
		int co = 0;
		while (it.hasNext()) {
			Configuration conf = it.next();
			patterns[co] = conf.getPattern();
			replacers[co] = conf.getReplacer();
			tries[co] = new Trie(new File(conf.getFile()), 1000, 0,
					Trie.Mode.SEARCH);
			// System.out.println(co);
			co++;
		}
		tries[size] = new Trie(new File("otherall.idx"), 1000, 0,
				Trie.Mode.SEARCH);
	}

	public long searchTrie(String key) {

		for (int j = 0; j < size; j++) {
			if (patterns[j].matcher(key).find()) {
				String str = key;
				Iterator<ReplacerEntry<String, String>> it = replacers[j]
						.iterator();
				while (it.hasNext()) {
					ReplacerEntry<String, String> entry = it.next();
					str = str.replace(entry.getKey(), entry.getValue());
				}
				KeyAnalyzer value = new KeyAnalyzer(str + "$", -1);
				tries[j].search(value);
				return value.getValue();
			}
		}

		KeyAnalyzer value = new KeyAnalyzer(key + "$", -1);
		tries[size].search(value);
		if (value.getValue() != -1)
			return value.getValue();

		return -1;
	}
	
//	public String searchBTree(long key){
//	
//		return bTreeMap.get(key);
//	}

	public void initBPTree() {
		// wai，B+树索引
		dbFile = new File("BPTreeDB");
		db = DBMaker.newFileDB(dbFile)
				.closeOnJvmShutdown()
				.writeAheadLogDisable()
		//		.transactionDisable()
				.randomAccessFileEnable()
		//		.encryptionEnable("password")
				.make();
		// open an collection, TreeMap has better performance then HashMap
		bTreeMap = db.getTreeMap("BPTree");
		//bTreeMap = db.getHashMap("hashTree");
		
		
		
		
//		File dbFile = new File("atoiDB");
//		File dbFile2 = new File("itoaDB");
//		//FileReader srcFile = new FileReader("/dawnfs/users/lwh/Jar/lubm/lubm_700.nt");
//
//		adb = DBMaker.newFileDB(dbFile)
//				.closeOnJvmShutdown()
//				.transactionDisable()
//		//		.randomAccessFileEnable()
//		//		.encryptionEnable("password")
//				.make();
//
//		idb = DBMaker.newFileDB(dbFile2)
//				.closeOnJvmShutdown()
//				.transactionDisable()
//		//		.randomAccessFileEnable()
//		//		.encryptionEnable("password")
//				.make();
//
//		aToiMap = adb.getHashMap("HTree");
//		//HTreeMap<Long, String> iToaMap = db2.getHashMap("hashTree");
//		iToaMap = idb.getTreeMap("BTree");
	}
	
	public void close(){
		db.close();
//		adb.close();
//		idb.close();
	}

	public void builtTree(String file, String newFile) throws IOException {

		BufferedOutputStream out = new BufferedOutputStream(
				new FileOutputStream(newFile));

		//File f = new File(file);
		FileReader fi = new FileReader(file);
		BufferedReader reader = new BufferedReader(fi);
		String line = null;
		String writeLine = "";
		String[] text;
		long key;
		
	//	int sign = 0;
		while ((line = reader.readLine()) != null) {
			text = line.split(" ");
			for (int i = 0; i < 3; i++) {

				try {
					if ((key = searchTrie(text[i])) != -1) {
//			if(bTreeMap.get(key) == null)
//				bTreeMap.put(key, text[i]);
						writeLine = writeLine + key + " ";
					} else {
						writeLine = writeLine + -1 + " ";
					}
				} catch (Exception e) {
					e.printStackTrace();
					writeLine = "$" + line;
				} 
				// System.out.println("adding:"+text[i]);
			}
			writeLine = writeLine + "\n";
			out.write(writeLine.getBytes());
			writeLine = "";
//			if(sign++ > 50000) {
//				db.commit();
//				sign = 0;
//			}
		}
//		reader.close();
//		db.commit();
//		fi = new FileReader(file);
//		reader = new BufferedReader(fi);
//		BufferedOutputStream out2 = new BufferedOutputStream(
//			new FileOutputStream("University2.txt"));
//		while ((line = reader.readLine()) != null) {
//			text = line.split(" ");
//			for (int i = 0; i < 3; i++) {
//
//				if ((key = search(text[i])) != -1) {
//					String val = bTreeMap.get(key);
//					if(!val.equalsIgnoreCase(text[i])){
//						System.out.println(key + "  " + text[i]);
//						System.out.println(key + "  " + val);
//					}else{
//						writeLine = writeLine + val + " ";
//					}
//				
//				
//				}
//				// System.out.println("adding:"+text[i]);
//			}
//			writeLine = writeLine + ".\n";
//			out2.write(writeLine.getBytes());
//			writeLine = "";
//		}
		
		
		
		reader.close();
		out.flush();
		out.close();
//		out2.flush();
//		out2.close();
		
		
		
		


//		FileReader fi = new FileReader(file);
//		BufferedReader reader = new BufferedReader(fi);
//		String line = null;
//		String[] text;
//		long key;
//		
//
//		while ((line = reader.readLine()) != null) {
//			text = line.split(" ");
//			for (int i = 0; i < 3; i++) {
//
//				if ((key = searchTrie(text[i])) != -1) {
//					if(iToaMap.get(key) == null){
//						iToaMap.put(key, text[i]);
//						aToiMap.put(text[i], key);
//					}					
//				} 
//			}
//		}
//
//		adb.commit();
//		idb.commit();
//	
//		reader.close();
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		 if(args.length != 3){
			 System.out.println("usage: config file newfile");
			 return;
		 }
		
		String conf = args[0];
		String file = args[1];
		String newFile = args[2];
//		String conf = "config.txt";
//		String file = "University.nt";
//		String newFile = "Int_University.txt";
//		String conf = "config.txt";
//		String file = "/dawnfs/users/lwh/Jar/lubm/lubm_700.nt";
//		String newFile = "Int_University2.txt";
		CreateBPTreeDemo demo = new CreateBPTreeDemo(conf);

		long start = System.currentTimeMillis();
		demo.builtTree(file, newFile);
		long end = System.currentTimeMillis();

		System.out.println(" Time: " + (end - start));
//		System.out.println(" counta: " + demo.aToiMap.size());
//		System.out.println(" counti: " + demo.iToaMap.size());
//		demo.close();

	}

}
