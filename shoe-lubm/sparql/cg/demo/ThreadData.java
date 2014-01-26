/**
 * 
 */
package cg.demo;

import cg.trie.KeyAnalyzer;

/**
 * @author C_G
 *
 */
public class ThreadData {
	private int prefix;
	private KeyAnalyzer key;
	private boolean end = false;
	public ThreadData() {}
	public ThreadData(int prefix, KeyAnalyzer key) {
		this(prefix, key, false);
	}
	public ThreadData(int prefix, KeyAnalyzer key, boolean end) {
		this.prefix = prefix;
		this.key = key;
		this.end = end;
	}
	public boolean isEnd() {
		return end;
	}
	public void setEnd(boolean end) {
		this.end = end;
	}
	public int getPrefix() {
		return prefix;
	}
	public void setPrefix(int prefix) {
		this.prefix = prefix;
	}
	public KeyAnalyzer getKey() {
		return key;
	}
	public void setKey(KeyAnalyzer key) {
		this.key = key;
	}
}
