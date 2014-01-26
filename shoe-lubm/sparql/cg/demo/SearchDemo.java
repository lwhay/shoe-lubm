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

import cg.demo.CreateTrieDemo.Configuration;
import cg.demo.CreateTrieDemo.Replacer;
import cg.demo.CreateTrieDemo.ReplacerEntry;
import cg.trie.KeyAnalyzer;
import cg.trie.Trie;

/**
 * @author C_G
 *
 */
public class SearchDemo {
	private int size;
	private Pattern[] patterns;
	private Trie[] tries;
	private Replacer[] replacers;

	// wai，B+树索引
	public File dbFile;
	public DB db;
	// open an collection, TreeMap has better performance then HashMap
	public BTreeMap<Long, String> bTreeMap;

	// public HTreeMap<Long, String> bTreeMap;
	public SearchDemo(String confPath){

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
				if (ss.length == 2) {
          replacer.put(ss[0], ss[1]);
        } else if (ss.length == 1) {
          replacer.put(ss[0], "");
        }
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
				if (ss.length == 2) {
          replacer.put(ss[0], ss[1]);
        } else if (ss.length == 1) {
          replacer.put(ss[0], "");
        }
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
		if (value.getValue() != -1) {
      return value.getValue();
    }

		return -1;
	}

	public void initBPTree() {
		// wai，B+树索引
//		dbFile = new File("/dawnfs/users/lwh/Jar/lubm/treeIndex/BPTreeDB");
		dbFile = new File("BPTreeDB");
		db = DBMaker.newFileDB(dbFile)
				.writeAheadLogDisable()
		//		.transactionDisable()
				.closeOnJvmShutdown()
		//		.encryptionEnable("password")
				.make();
		// open an collection, TreeMap has better performance then HashMap
		bTreeMap = db.getTreeMap("BPTree");
		// bTreeMap = db.getHashMap("hashTree");
	}


	public String searchBTree(long key) {

	  if (null == bTreeMap) {
      System.out.println("***********null*****");
      return new String("-1");
    }
	  if (bTreeMap.isEmpty()) {
      System.out.println("***********empty*****");
    }

		return bTreeMap.get(key);
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		SearchDemo demo = new SearchDemo("config.txt");
//		String[] str = {"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
//				"<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent>",
//				"<http://www.Department0.University0.edu/GraduateCourse0>",
//				"<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor>",
//				"<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor>",
//				"<http://www.Department0.University0.edu>",
//				"<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name>",
//				"<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress>",
//				"<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#telephone>",
//				"<http://www.Department0.University0.edu/FullProfessor0>",
//				"<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department>",
//				"<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf>",
//				"<http://www.University0.edu>",
//				"<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf>",
//				"<http://www.Department6.University0.edu>",
//				"<http://www.Department3.University0.edu>",
//				"<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course>",
//				"<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor>",
//				"<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf>",
//				"<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse>"};
//		for(String st : str){
		String st = args[0];
			System.out.println(st + " -->> " +demo.searchTrie(st));
//		}

	}

}
