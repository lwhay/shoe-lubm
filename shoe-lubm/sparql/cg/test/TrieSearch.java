/**
 * 
 */
package cg.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cg.trie.KeyAnalyzer;
import cg.trie.Trie;

/**
 * @author C_G
 *
 */
public class TrieSearch {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		Trie trie = new Trie(new File("lubm_4.idx"), 1000, 0, Trie.Mode.SEARCH);
		BufferedReader br = new BufferedReader(new FileReader(new File("rdf.nt")));
		String line = "";
		long count = -2;
		long lines = 0;
		long start = System.currentTimeMillis();
		Pattern pattern = Pattern.compile("^<http://www.Department[8-9]");
		Matcher matcher = null;
		while((line = br.readLine()) != null) {
			String splits[] = line.split(" ");
			for (int i = 0; i < 3; i++) {
				matcher = pattern.matcher(splits[i]);
				if (matcher.find()) {
//					System.out.println(Thread.currentThread().getName() + splits[i]);
					KeyAnalyzer key = new KeyAnalyzer(splits[i]+"$", count);
					trie.search(key);
					if (key.getValue() == -2) {
						System.out.println(key.toString());
					}
				}
//			}
//			KeyAnalyzer key = new KeyAnalyzer(splits[i]+"$", count);
//			trie.search(key);
//			if (key.getValue() == -2) {
//				System.out.println(key.toString());
//			}
			}
			lines++;
			if (lines % 10000 == 0)  {
				long tmp = System.currentTimeMillis();
				System.out.println("lines: "+ lines + " Time: " + (tmp - start));
			}
		}
		long end = System.currentTimeMillis();
		System.out.println("Time: " + (end - start));
		trie.close();

	}

}
