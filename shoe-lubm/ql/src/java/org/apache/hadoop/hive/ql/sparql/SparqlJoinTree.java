package org.apache.hadoop.hive.ql.sparql;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hive.ql.parse.QBJoinTree;

/**
 * @作者 张仲伟
 * @创建时间 2012-9-1 下午5:03:28
 * @项目名称 Sparql
 * @联系方式 wai1316@qq.com
 */
@SuppressWarnings("serial")
public class SparqlJoinTree extends QBJoinTree{

	private QGEdge joininfo;
	private final List<String[]> joinKeys;
	public SparqlJoinTree() {
		// TODO Auto-generated constructor stub
		super();
	//	joinKeys = new HashMap<String, String>();
		joinKeys = new LinkedList<String[]>();
	}

	public List<String[]> getJoinKeys() {
		return joinKeys;
	}

	public void addJoinKeys(String table, String key){
		String[] en = new String[2];
		en[0] = table;
		en[1] = key;
		joinKeys.add(en);
	}

	public void addJoinKeys(String[] en){
		joinKeys.add(en);
	}

	public QGEdge getJoininfo() {
		return joininfo;
	}

	public void setJoininfo(QGEdge joininfo) {
		this.joininfo = joininfo;
	}


}
