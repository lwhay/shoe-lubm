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
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cg.trie.KeyAnalyzer;
import cg.trie.Trie;

/**
 * @author C_G
 * 
 */
public class CreateTrieDemo {
	private List<Configuration> confs;
	private String file;
	public CreateTrieDemo(String file, List<Configuration> confs) {
		this.file = file;
		this.confs = confs;
	}
	public void start() {
		Pattern patterns[] = new Pattern[confs.size()];
		Iterator<Configuration> it = confs.iterator();
		int cc = 0;
		while (it.hasNext()) {
			Configuration conf = it.next();
			patterns[cc] = conf.getPattern();
			CreateTrie trie = new CreateTrie(conf.getFile(), 1000, 5000, patterns[cc], conf.getReplacer(), file);
			new Thread(trie, conf.getThread()).start();
			cc++;
		}
		try {
			Trie trie = new Trie(new File("otherall.idx"), 1000, 5000, Trie.Mode.CREATE);
			BufferedReader br = new BufferedReader(new FileReader(file));
			long value = 0;
			String line = null;
			long lines = 0;
			long start = System.currentTimeMillis();
			while ((line = br.readLine()) != null) {
				String splits[] = line.split(" ");
				for (int i = 0; i < 3; i++) {
					boolean find = false;
					for (int j = 0; j < patterns.length; j++) {
						if (patterns[j].matcher(splits[i]).find()) {
							find = true;
						}
					}
					if (!find) {
						//System.out.println(splits[i]);
						KeyAnalyzer key = new KeyAnalyzer(splits[i]+"$", value * 100 + 0);
						if (trie.insert(key))
							value++;
					}
				}
				lines++;
				if (lines % 10000 == 0) {
					long tmp = System.currentTimeMillis();
					System.out.println("Main--->Lines: " + lines +" Count: " + value + " Time: " + (tmp - start));
				}
			}
			long end = System.currentTimeMillis();
			System.out.println("Main--->lines: " + lines + "count: " + value + " Time: " + (end - start));
			trie.close();
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public static void main(String[] args) throws IOException {

//		if(args.length != 2){
//			System.out.println("usage: config file");
//			return;
//		}
//		
//		String pfile = args[0];
//		String file = args[1];		
		
		String pfile = "config.txt";
		String file = "University.nt";
		
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
		
		new CreateTrieDemo(file, confs).start();
	}

	/**
	 * @param args
	 */

	static class CreateTrie implements Runnable {
		private Trie trie;
		private File file;
		private static int count = 0;
		private int id;
		private String rfile;
		private Pattern pattern;
		private Replacer replacer;
		public CreateTrie(String sfile, int size, int bsize, Pattern pattern, Replacer replacer, String rfile) {
			
			count++;
			id = count;
			file = new File(sfile);
			trie = new Trie(file, size, bsize, Trie.Mode.CREATE);
			this.rfile = rfile;
			this.pattern = pattern;
			this.replacer = replacer;
		}

		@Override
		public void run() {
			try {
				BufferedReader br = new BufferedReader(new FileReader(rfile));
				long value = 0;
				String line = null;
				long lines = 0;
				Matcher matcher = null;
				long start = System.currentTimeMillis();
				while ((line = br.readLine()) != null) {
					String splits[] = line.split(" ");
					for (int i = 0; i < 3; i++) {	
						matcher = pattern.matcher(splits[i]);
						if (matcher.find()) {
//							System.out.println(Thread.currentThread().getName() + splits[i]);
//							KeyAnalyzer key = new KeyAnalyzer(matcher.replaceAll("")+"$", value * 10 + id);
							String str = splits[i];
							Iterator<ReplacerEntry<String, String>> it = replacer.iterator();
							while (it.hasNext()) {
								ReplacerEntry<String, String> entry = it.next();
								str = str.replace(entry.getKey(), entry.getValue());
							}
						//	System.out.println(splits[i]+"; "+str);
//							str.replace("\"", "");
//							str.replace("<", "");
//							str.replace(">", "");
							KeyAnalyzer key = new KeyAnalyzer(str+"$", value * 100 + id);
							if (trie.insert(key))
								value++;
						}
					}
					lines++;
					if (lines % 10000 == 0) {
						long tmp = System.currentTimeMillis();
						System.out.println(Thread.currentThread().getName() + "--->Lines: " + lines +" Count: " + value + " Time: " + (tmp - start));
					}
				}
				long end = System.currentTimeMillis();
				System.out.println(Thread.currentThread().getName() + 
						"--->lines: " + lines +"count: " + value + " Time: " + (end - start));
				trie.close();
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
				
		}

	}
	public static class Replacer {
		List<ReplacerEntry<String, String>> entries;
		
		public Replacer() {
			entries = new ArrayList<ReplacerEntry<String, String>>();
		}
		
		public Iterator<ReplacerEntry<String, String>> iterator() {
			return entries.iterator();
		}
		public void put(String key, String value) {
			ReplacerEntry<String, String> entry = new ReplacerEntry<String, String>(key, value);
			entries.add(entry);
		}
	}
	public static class ReplacerEntry<K, V> implements Map.Entry<K, V> {
		private K key;
		private V value;
		public ReplacerEntry() {}
		public ReplacerEntry(K key, V value) {
			this.key = key;
			this.value = value;
		}
		@Override
		public K getKey() {
			return key;
		}

		@Override
		public V getValue() {
			return value;
		}

		@Override
		public V setValue(V value) {
			V old = this.value;
			this.value = value;
			return old;
		}
		
		public K setKey(K key) {
			K old = this.key;
			this.key = key;
			return old;
		}
		
	}
	public static class Configuration {
		private Replacer replacer;
		private Pattern pattern;
		private String thread;
		private String file;
		public Replacer getReplacer() {
			return replacer;
		}
		public void setReplacer(Replacer replacer) {
			this.replacer = replacer;
		}
		public Pattern getPattern() {
			return pattern;
		}
		public void setPattern(Pattern pattern) {
			this.pattern = pattern;
		}
		public String getThread() {
			return thread;
		}
		public void setThread(String thread) {
			this.thread = thread;
		}
		public String getFile() {
			return file;
		}
		public void setFile(String file) {
			this.file = file;
		}
	}
}
