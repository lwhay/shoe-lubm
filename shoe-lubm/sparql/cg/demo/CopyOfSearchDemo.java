/**
 * 
 */
package cg.demo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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
public class CopyOfSearchDemo {
	private int size;
	private Pattern[] patterns;
	private Trie[] tries;
	private Replacer[] replacers;

	// wai，B+树索引
	public File dbFile;
	public DB db;
	// open an collection, TreeMap has better performance then HashMap
	public BTreeMap<Long, String> bTreeMap;
	
	// wai，B+树索引
	public File dbFile2;
	public DB adb;
	// open an collection, TreeMap has better performance then HashMap
	public HTreeMap<String, Long> atoimap;
	
	// wai，B+树索引
	public File dbFile3;
	public DB idb;
	// open an collection, TreeMap has better performance then HashMap
	public BTreeMap<Long, String> itoamap;

	// public HTreeMap<Long, String> bTreeMap;
	public CopyOfSearchDemo(String confPath){

		List<Configuration> confs = new ArrayList<Configuration>();

		try {
			initConfig(confs, confPath);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		initTrie(confs);

		initBPTree();

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

	public List<Configuration> initConfig(String confPath)
			throws IOException {

		List<Configuration> confs = new ArrayList<Configuration>();
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
		
		return confs;
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
			String filepath = conf.getFile();
//			filepath = "/dawnfs/users/lwh/Jar/lubm/treeIndex/"+filepath;
			tries[co] = new Trie(new File(filepath), 1000, 0,
					Trie.Mode.SEARCH);
			// System.out.println(co);
			co++;
		}
//		tries[size] = new Trie(new File("/dawnfs/users/lwh/Jar/lubm/treeIndex/otherall.idx"), 1000, 0,
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
	
	public void initBPTree() {
		// wai，B+树索引
//		dbFile = new File("/dawnfs/users/lwh/Jar/lubm/treeIndex/BPTreeDB");
		dbFile = new File("BPTreeDB");
		db = DBMaker.newFileDB(dbFile)
		//		.transactionDisable()
				.writeAheadLogDisable()
				.closeOnJvmShutdown()
				.randomAccessFileEnable()
		//		.encryptionEnable("password")
				.make();
		// open an collection, TreeMap has better performance then HashMap
		bTreeMap = db.getTreeMap("BPTree");
		// bTreeMap = db.getHashMap("hashTree");
		
		File dbFile = new File("atoiDB");
		File dbFile2 = new File("itoaDB");
//		File dbFile = new File("/dawnfs/users/lwh/Jar/lubm/treeIndex/atoiDB");
//		File dbFile2 = new File("/dawnfs/users/lwh/Jar/lubm/treeIndex/itoaDB");
		adb = DBMaker.newFileDB(dbFile)
				.closeOnJvmShutdown()
				.writeAheadLogDisable()
		//		.transactionDisable()
				.randomAccessFileEnable()
		//		.encryptionEnable("password")
				.make();

		idb = DBMaker.newFileDB(dbFile2)
				.closeOnJvmShutdown()
				.writeAheadLogDisable()
		//		.transactionDisable()
				.randomAccessFileEnable()
		//		.encryptionEnable("password")
				.make();

		atoimap = adb.getHashMap("HTree");
		//HTreeMap<Long, String> iToaMap = db2.getHashMap("hashTree");
		itoamap = idb.getTreeMap("BTree");
	}

		
	public String searchBTree(long key) {

		return bTreeMap.get(key);
	}
	public String searchITree(long key) {

		return itoamap.get(key);
	}
	public Long searchATree(String key) {

		return atoimap.get(key);
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		CopyOfSearchDemo demo = new CopyOfSearchDemo("config.txt");
		
		long times = System.currentTimeMillis();		
		FileReader srcFile = new FileReader("/dawnfs/users/lwh/Jar/lubm/lubm_700.nt");
//		FileReader srcFile = new FileReader("University.nt");
		BufferedReader reader = new BufferedReader(srcFile);
		
		String line = null;
		String[] text;
		long abc = 20;
		while ((line = reader.readLine()) != null) {
			text = line.split(" ");
			for (int i = 0; i < 3; i++) {
				if(demo.searchTrie(text[i]) == -1)
					System.out.println("trie miss text: " + text[i]);
			}
			if(abc-- < 0) break;
		}
		reader.close();
		System.out.println("Trie time: " + ( System.currentTimeMillis() - times ));
		
	//	System.in.read();
		abc = 20;
		times = System.currentTimeMillis();		
		srcFile = new FileReader("/dawnfs/users/lwh/Jar/lubm/lubm_700.nt");
//		srcFile = new FileReader("University.nt");
		reader = new BufferedReader(srcFile);
		System.out.println("atoi size: " + demo.atoimap.size());
		line = null;
		while ((line = reader.readLine()) != null) {
			text = line.split(" ");
			for (int i = 0; i < 3; i++) {
				if(demo.searchATree(text[i]) == null)
					System.out.println("atoi miss text: " + text[i]);
			}
			if(abc-- < 0) break;
		}
		reader.close();
		System.out.println("atoi time: " + ( System.currentTimeMillis() - times ));
		
		System.in.read();
		
		times = System.currentTimeMillis();		
		long kk = demo.itoamap.size();
		
		while (kk > 0) {
			String ll = demo.searchITree(kk);
			if(ll == null){
				System.out.println("itoa miss int: " + kk);
				kk--;
				continue;
			}
			if(demo.searchATree(ll) == null)
				System.out.println("itoa miss text: " + ll);
			kk--;
		}
		System.out.println("itoa size: " + demo.itoamap.size());
		System.out.println("atoi time: " + ( System.currentTimeMillis() - times ));
		
		System.in.read();
		
		times = System.currentTimeMillis();		
		kk = demo.bTreeMap.size();
		System.out.println("itoa size: " + kk);
		while (kk > 0) {
			String ll = demo.searchBTree(kk);
			if(ll == null){
				System.out.println("bt miss int: " + kk);
				kk--;
				continue;
			}
			if(demo.searchATree(ll) == null)
				System.out.println("bt miss text: " + ll);
			kk--;
		}
		System.out.println("atoi time: " + ( System.currentTimeMillis() - times ));
	}

}
