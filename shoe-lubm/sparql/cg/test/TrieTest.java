/**
 * 
 */
package cg.test;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;

import cg.trie.InterNode;
import cg.trie.KeyAnalyzer;
import cg.trie.LeafNode;
import cg.trie.KeyAnalyzer;
import cg.trie.TrieCache;

/**
 * @author C_G
 *
 */
public class TrieTest {
	public TrieCache cache;
	public boolean search(KeyAnalyzer keys, long iblock) {
		InterNode inode = (InterNode)cache.get(iblock);
		byte key = keys.key();
//		System.out.print((char)key);
		long child = inode.child(key);
//		if (child == 1283281)
//			System.out.println();
		if (-1 == child ) {
//			System.out.println(blk.toString());
 			System.out.println("not contain the key!!!");
			return false;
		} else {
			if (key == KeyAnalyzer.ENDMARK) {
				LeafNode ret = (LeafNode) cache.get(child);
				keys.setValue(ret.getValue());
				return true;
			} else {
				return search(keys, child);
			}
			
		}
	}
	public boolean insert(KeyAnalyzer keys, long iblock) {
		InterNode inode = (InterNode) cache.get(iblock);
//		System.out.println(inode.toString());
//		if ((char)inode.key() == 'h')
//			System.out.println();
		byte key = keys.key();
		long child = inode.child(key);
		if (-1 == child && key != KeyAnalyzer.ENDMARK) {
			InterNode inode0 = new InterNode(key);
			cache.put(inode0);
			long seek = inode0.seek();	
			inode.child(key, seek);
			inode.dirty(true);
			return insert(keys, seek);
		} else if (-1 == child && key == KeyAnalyzer.ENDMARK) {
			LeafNode lnode = new LeafNode(keys.getValue());
			cache.put(lnode);
			long seek = lnode.seek();	
			inode.child(key, seek);
			inode.dirty(true);
			return true;
		} else if (key == KeyAnalyzer.ENDMARK) {
			return false;
		} else {
			return insert(keys, child);
		}
	}
	public static void main(String[] args) throws IOException {
		TrieTest trie = new TrieTest();
		RandomAccessFile file = new RandomAccessFile(new File("lubm.idx"), "rw");
//		PrintStream out = new PrintStream(new BufferedOutputStream(new FileOutputStream("index.log")));
//		System.setOut(out);
//		file.setLength(0);
		trie.cache = new TrieCache(file, 1000, 5000);
		InterNode inode0 = new InterNode(KeyAnalyzer.ENDMARK);
		trie.cache.put(inode0);
//		
		BufferedReader br = new BufferedReader(new FileReader(new File("rdf.nt")));
		String line = "";
		long count = 0;
		long lines = 0;
		long start = System.currentTimeMillis();
		while((line = br.readLine()) != null) {
			String splits[] = line.split(" ");
			KeyAnalyzer entry0 = new KeyAnalyzer(splits[0]+"$", count);
//			trie.search(entry0, 0);
//			if (entry0.getValue() == -2)
//				System.out.println(entry0.toString());
//			System.out.print(entry0.getValue()+" ");
//			System.out.println(entry0.toString());
			if(trie.insert(entry0, 0))
				count++;
			KeyAnalyzer entry1 = new KeyAnalyzer(splits[1]+"$", count);
//			trie.search(entry1, 0);
//			if (entry1.getValue() == -2)
//				System.out.println(entry1.toString());
//			System.out.print(entry1.getValue()+" ");
//			System.out.println(entry1.toString());
			if (trie.insert(entry1, 0))
				count++;
			KeyAnalyzer entry2 = new KeyAnalyzer(splits[2]+"$", count);
//			trie.search(entry2, 0);
//			if (entry1.getValue() == -2)
//				System.out.println(entry0.toString());
//			System.out.print(entry2.getValue());
//			System.out.println(entry2.toString());
			if (trie.insert(entry2, 0))
				count++;
			lines++;
			if (lines % 10000 == 0)  {
				long tmp = System.currentTimeMillis();
				System.out.println("lines: "+lines+"count: " + count + " Time: " + (tmp - start));
			}
		//	System.out.println();
		}
		long end = System.currentTimeMillis();
		System.out.println("count: " + count + " Time: " + (end - start));
//		System.out.println(key1.getValue());
		trie.cache.close();
//		out.close();
//		System.setOut(System.out);
	}
}
