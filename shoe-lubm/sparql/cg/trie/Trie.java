/**
 * 
 */
package cg.trie;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author C_G
 * 
 */
public class Trie {

	private TrieCache cache;
	private RandomAccessFile file;
	private int size;
	private long root = 0;
	private int bsize;
	public Trie(File sfile, int size, int bsize, int mode) {
		this.size = size;
		this.bsize = bsize;
		try {
			file = new RandomAccessFile(sfile, "rw");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		if (mode == Mode.CREATE)
			create();
		else if (mode == Mode.APPEND)
			append();
		else if (mode == Mode.SEARCH)
			search();
	}

	public boolean insert(KeyAnalyzer key) {
		return insert(key, root);
	}

	private boolean insert(KeyAnalyzer keys, long iblock) {
		InterNode inode = (InterNode) cache.get(iblock);
//		System.out.println(inode.toString());
		// if ((char)inode.key() == 'h')
		// System.out.println();
		byte key = keys.key();
		long child = inode.child(key);
		if (-1 == child && key != KeyAnalyzer.ENDMARK) {
			InterNode inode0 = new InterNode(key);
			cache.put(inode0, inode);
			long seek = inode0.seek();
			inode.child(key, seek);
			inode.dirty(true);
			return insert(keys, seek);
		} else if (-1 == child && key == KeyAnalyzer.ENDMARK) {
			LeafNode lnode = new LeafNode(keys.getValue());
			cache.put(lnode, inode);
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

	public boolean search(KeyAnalyzer key) {
		return search(key, root);
	}

	private boolean search(KeyAnalyzer keys, long iblock) {
		InterNode inode = (InterNode) cache.get(iblock);
		byte key = keys.key();
		// System.out.print((char)key);
		long child = inode.child(key);
		// if (child == 1283281)
		// System.out.println();
		if (-1 == child) {
			System.out.println(inode.toString());
			System.out.println("not contain the key!!!");
			System.out.println((char)key);
			return false;
		} else {
			if (key == KeyAnalyzer.ENDMARK) {
				LeafNode ret;
				try {
					ret = (LeafNode) cache.get(child);
					keys.setValue(ret.getValue());
				} catch (ClassCastException e) {
				//	System.out.println(keys.getKey());
					throw new ClassCastException(keys.getKey());
				}
				
				return true;
			} else {
				return search(keys, child);
			}

		}
	}
	public void close() {
		cache.close();
	}
	private void create() {
		try {
			file.setLength(0);
		} catch (IOException e) {
			e.printStackTrace();
		}
		cache = new TrieCache(file, size, bsize);
		InterNode inode0 = new InterNode(KeyAnalyzer.ENDMARK);
		inode0.seek(root);
		cache.put(inode0);
	}

	private void search() {
		cache = new TrieCache(file, size, 0);
	}

	private void append() {

	}

	public static interface Mode {
		public static int CREATE = 0;
		public static int APPEND = 1;
		public static int SEARCH = 2;
	}
}
