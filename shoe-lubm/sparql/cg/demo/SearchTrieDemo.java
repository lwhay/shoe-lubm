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

import cg.demo.CreateTrieDemo.Configuration;
import cg.demo.CreateTrieDemo.Replacer;
import cg.demo.CreateTrieDemo.ReplacerEntry;
import cg.trie.KeyAnalyzer;
import cg.trie.Trie;

/**
 * @author C_G
 * 
 */
public class SearchTrieDemo {
	private int size;
	private Pattern[] patterns;
	private Trie[] tries;
	private Replacer[] replacers;
	
	private static String file;
	private static String pfile;

	public SearchTrieDemo(List<Configuration> confs) {
		size = confs.size();
//		System.out.println(size);
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
			filepath = "/dawnfs/users/lwh/Jar/lubm/treeIndex/"+filepath;
			tries[co] = new Trie(new File(filepath), 1000, 0,
					Trie.Mode.SEARCH);
//			System.out.println(co);
			co++;
		}
		tries[size] = new Trie(new File("/dawnfs/users/lwh/Jar/lubm/treeIndex/otherall.idx"), 1000, 0,
//		tries[size] = new Trie(new File("otherall.idx"), 1000, 0,
				Trie.Mode.SEARCH);
	}

	public void search(String key) {
		boolean find = false;
		for (int j = 0; j < size; j++) {
			if (patterns[j].matcher(key).find()) {
				String str = key;
				Iterator<ReplacerEntry<String, String>> it = replacers[j].iterator();
				while (it.hasNext()) {
					ReplacerEntry<String, String> entry = it.next();
					str = str.replace(entry.getKey(), entry.getValue());
				}
				KeyAnalyzer value = new KeyAnalyzer(str+"$", -1);
				tries[j].search(value);
				if (value.getValue() == -1)
					System.out.println(value.toString());
				find = true;
				break;
			}
		}
		if (!find) {
			KeyAnalyzer value = new KeyAnalyzer(key+"$", -1);
			tries[size].search(value);
			if (value.getValue() == -1)
				System.out.println(value.toString());
		}

	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
//		String pfile = "";
//		String file = "";
//		for (int i = 0; i < args.length; i++) {
//			if ("-file".compareToIgnoreCase(args[i]) == 0) {
//				i++;
//				file = args[i];
//			}
//			if ("-conf".compareToIgnoreCase(args[i]) == 0) {
//				i++;
//				pfile = args[i];
//			}
//		}
		pfile = "/dawnfs/users/lwh/Jar/lubm/treeIndex/config.txt";
		file = "/dawnfs/users/lwh/Jar/lubm/lubm_700.nt";
		List<Configuration> confs = new ArrayList<Configuration>();
		BufferedReader pread = new BufferedReader(new FileReader(pfile));
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
				if(ss.length == 2 )
					replacer.put(ss[0], ss[1]);
				else if(ss.length == 1 )
					replacer.put(ss[0], "");
			}
			conf.setReplacer(replacer);
			conf.setThread("Thread#" + thread);
			thread++;
			confs.add(conf);
		}
		pread.close();
		SearchTrieDemo s = new SearchTrieDemo(confs);
		BufferedReader br = new BufferedReader(new FileReader(file));
		String line = null;
		
		long start = System.currentTimeMillis();
		int kk = 0;
		while ((line = br.readLine()) != null) {
			String splits[] = line.split(" ");
			s.search(splits[0]);
			s.search(splits[1]);
			s.search(splits[2]);
			if(kk++ >1000) 
				break;
		}
		long end = System.currentTimeMillis();
		System.out.println(" Time: " + (end - start));
		br.close();
	}

}
