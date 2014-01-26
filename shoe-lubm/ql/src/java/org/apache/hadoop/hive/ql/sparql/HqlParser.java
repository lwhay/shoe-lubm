/**
 *
 */
package org.apache.hadoop.hive.ql.sparql;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author C_G
 *
 */
public class HqlParser {
	private String hql = null;
	private final Pattern pattern;

	public HqlParser(String hql) {
		this(hql, "(prefix foaf:)?.*select.*(\\n)?where\\s?\\{.*\\}");
	}

	public HqlParser(String hql, String regex) {
		this.hql = hql.toLowerCase();
		pattern = Pattern.compile(regex);
	}

	public boolean isHql() {
		Matcher matcher = pattern.matcher(hql);
		if (matcher.find()) {
      return false;
    }
		return true;
	}

	public static void main(String args[]) {
		System.out.println(new HqlParser(
				"prefix foaf: hello select ?name where {dd}").isHql());
		System.out.println(new HqlParser(
				"select ?name where {\"asdf\" ?asfd ?s}").isHql());
		System.out.println(new HqlParser("select name from where").isHql());
		System.out.println(new HqlParser("select name from").isHql());
	}
}
