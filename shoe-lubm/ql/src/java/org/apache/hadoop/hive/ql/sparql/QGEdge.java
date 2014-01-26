package org.apache.hadoop.hive.ql.sparql;

import java.util.List;

/**
 * @作者 张仲伟
 * @时间 2012-8-28 上午11:22:20
 * @项目 Sparql
 * @联系方式 wai1316@qq.com
 */
public class QGEdge {

//	//所存在的各种join形式
//	public static final int S_S = 1;
//	public static final int S_P = 2;
//	public static final int S_O = 3;
//	public static final int P_S = 4;
//	public static final int P_P = 5;
//	public static final int P_O = 6;
//	public static final int O_S = 7;
//	public static final int O_P = 8;
//	public static final int O_O = 9;

	//边的ID
	public int edgeId = -1;

	//边的两个顶点：（S P O）三元组
	public QGNode first;
	public QGNode second;

	//两个顶点的join形式，有可能存在2个
	//public List<Integer> join;
	public List<String[]> joins;

	public QGEdge(QGNode first, QGNode second, List<String[]> joins) {
		this.first = first;
		this.second = second;
		this.joins = joins;
	}

	/**
	 * @return the edgeId
	 */
	public int getEdgeId() {
		return edgeId;
	}

	/**
	 * @param edgeId
	 *            the edgeId to set
	 */
	public void setEdgeId(int edgeId) {
		this.edgeId = edgeId;
	}

}
