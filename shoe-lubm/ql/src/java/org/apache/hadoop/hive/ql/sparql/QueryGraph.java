package org.apache.hadoop.hive.ql.sparql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.QB;

public class QueryGraph extends QB {

	// 顶点和边
	private HashMap<Integer, QGNode> nodes;
	private HashMap<Integer, QGEdge> edges;

	// 需要投影的属性
	private HashMap<Integer, String> projections;
	private HashMap<Integer, String> projectionToAlias;
	private HashMap<Integer, String> projectionToTable;

	// 表的别名以及别名所对应的真实表对象
	private final HashMap<Integer, String> aliases;
	private final HashMap<String, Table> aliasToTable;
	private final HashSet<String> existAliases;

	// 过滤条件
	private final HashMap<String, HashMap<String, String>> filterKeys;

	// 各种统计信息
	private int projectionsNums = 0;
	private int nodesNums = 0;
	private int edgesNums = 0;

	// private SparqlJoinTree joinTree;

	// 保存在数据库中真实的表名
	public static final String tableName = "sparqltable";
	public static final String stableName = "stable";
	public static final String otableName = "otable";
	public static final long  digi = 50000;

	public QueryGraph() {
		super(null, null, false);
		nodes = new HashMap<Integer, QGNode>();
		edges = new HashMap<Integer, QGEdge>();
		projections = new HashMap<Integer, String>();
		projectionToAlias = new HashMap<Integer, String>();
		projectionToTable = new HashMap<Integer, String>();
		aliases = new HashMap<Integer, String>();
		aliasToTable = new HashMap<String, Table>();
		existAliases = new HashSet<String>();
		filterKeys = new HashMap<String, HashMap<String,String>>();
	}

	public QueryGraph(String outer_id, String alias, boolean isSubQ) {
		super(outer_id, alias, isSubQ);
		nodes = new HashMap<Integer, QGNode>();
		edges = new HashMap<Integer, QGEdge>();
		projections = new HashMap<Integer, String>();
		projectionToAlias = new HashMap<Integer, String>();
		projectionToTable = new HashMap<Integer, String>();
		aliases = new HashMap<Integer, String>();
		aliasToTable = new HashMap<String, Table>();
		existAliases = new HashSet<String>();
		filterKeys = new HashMap<String, HashMap<String,String>>();
	}

	public void addNode(QGNode node) {
		String alias = "tb" + nodesNums;

		String tabIdName = node.tableName;

		node.setNodeId(nodesNums);
		node.setAlias(alias);

		nodes.put(nodesNums, node);
		aliases.put(nodesNums, alias);

		// Insert this map into the stats
		super.setTabAlias(alias, tabIdName);
		super.addAlias(alias);

		nodesNums++;
	}

	public void addEdge(QGEdge edge) {

		String left = edge.first.alias;
		String right = edge.second.alias;

		// 如果两边的表已经存在，结束
		if(existAliases.contains(left) && existAliases.contains(right)){
			//这里应该添加一些处理，把不存在的join添加到前一个edge中
			return;
		}

		// 如果只是右边的表已经存在，交换位置
		if(!existAliases.contains(left) && existAliases.contains(right)){

			QGNode node = edge.first;
			edge.first = edge.second;
			edge.second = node;

			List<String[]> join = new ArrayList<String[]>();
			for(int i = 0; i< edge.joins.size(); i++){
				String[] joinKey = edge.joins.get(i);
				String[] newjoinKey = new String[4];
				newjoinKey[0] = joinKey[2];
				newjoinKey[1] = joinKey[3];
				newjoinKey[2] = joinKey[0];
				newjoinKey[3] = joinKey[1];
				join.add(newjoinKey);
			}
			edge.joins = join;
		}
		// 如果只是左边的表已经存在，暂不处理
		if(existAliases.contains(left) && !existAliases.contains(right)){

		}
		existAliases.add(left);
		existAliases.add(right);
		edge.setEdgeId(edgesNums);

		edges.put(edgesNums, edge);
		edgesNums++;
	}

	public void addProjection(String name) {
		projections.put(projectionsNums, name);
		projectionsNums++;
	}

	/**
	 * 处理projection 为了找出那些投影是属于哪一个pattern
	 *
	 */
	public void processProjections() {
		for (int i = 0; i < projectionsNums; i++) {
			if (nodesNums != 0) {
				for (int j = 0; j < nodesNums; j++) {
					QGNode node = nodes.get(j);
					String projection = projections.get(i);
					if (projection.compareToIgnoreCase(node.subject) == 0) {
						projectionToAlias.put(i, "S");
						projectionToTable.put(i, node.alias);
						break;
					} else if (projection.compareToIgnoreCase(node.predicate) == 0) {
						projectionToAlias.put(i, "P");
						projectionToTable.put(i, node.alias);
						break;
					} else if (projection.compareToIgnoreCase(node.object) == 0) {
						projectionToAlias.put(i, "O");
						projectionToTable.put(i, node.alias);
						break;
					}
				}
			} else {
				QueryGraph qg = (QueryGraph)this.getSubqForAlias("tmp").getQBExpr1().getQB();
				for (int j = 0; j < qg.nodesNums; j++) {
					QGNode node = qg.nodes.get(j);
					String projection = projections.get(i);
					if (projection.compareToIgnoreCase(node.subject) == 0) {
						projectionToAlias.put(i, "S");
						projectionToTable.put(i, node.alias);
						break;
					} else if (projection.compareToIgnoreCase(node.predicate) == 0) {
						projectionToAlias.put(i, "P");
						projectionToTable.put(i, node.alias);
						break;
					} else if (projection.compareToIgnoreCase(node.object) == 0) {
						projectionToAlias.put(i, "O");
						projectionToTable.put(i, node.alias);
						break;
					}
				}

			}
		}
	}

	/**
	 * 处理连接where中的所有连接，以图的边形式表达，并返回
	 * 连接关键字
	 */
	public void buildEdges() {
		for (QGNode first : nodes.values()) {
			for (QGNode second : nodes.values()) {
				if (first.nodeId >= second.nodeId) {
          continue;
        }

				List<String[]> join = new ArrayList<String[]>();
				if (isIntersect(first, second, join)) {
					QGEdge edge = new QGEdge(first, second, join);
					addEdge(edge);
				}
			}
		}
	}

	/**
	 * 判断每个节点是否相交，并返回连接关键字
	 *
	 */
	public boolean isIntersect(QGNode first, QGNode second, List<String[]> join) {
		// 常量相同只能算是选择不能当连接
		boolean result = false;

		HashMap<Integer, String> bindings1 = new HashMap<Integer, String>();
		HashMap<Integer, String> bindings2 = new HashMap<Integer, String>();

		if (!first.constSubject) {
			bindings1.put(0, first.subject);
		}
		if (!first.constPredicate) {
			bindings1.put(1, first.predicate);
		}
		if (!first.constObject) {
			bindings1.put(2, first.object);
		}
		if (!second.constSubject) {
			bindings2.put(0, second.subject);
		}
		if (!second.constPredicate) {
			bindings2.put(1, second.predicate);
		}
		if (!second.constObject) {
			bindings2.put(2, second.object);
		}

		for (Entry<Integer, String> entry1 : bindings1.entrySet()) {
			for (Entry<Integer, String> entry2 : bindings2.entrySet()) {
				if (entry1.getValue().equalsIgnoreCase(entry2.getValue())) {
					if (entry1.getKey() == 0 && entry2.getKey() == 0){
						String[] joinkey = new String[4];
						joinkey[0] = first.alias;
						joinkey[1] = "S";
						joinkey[2] = second.alias;
						joinkey[3] = "S";
						join.add(joinkey);
					}
					else if (entry1.getKey() == 0 && entry2.getKey() == 1){
						String[] joinkey = new String[4];
						joinkey[0] = first.alias;
						joinkey[1] = "S";
						joinkey[2] = second.alias;
						joinkey[3] = "P";
						join.add(joinkey);
					}
					else if (entry1.getKey() == 0 && entry2.getKey() == 2){
						String[] joinkey = new String[4];
						joinkey[0] = first.alias;
						joinkey[1] = "S";
						joinkey[2] = second.alias;
						joinkey[3] = "O";
						join.add(joinkey);
					}
					else if (entry1.getKey() == 1 && entry2.getKey() == 0){
						String[] joinkey = new String[4];
						joinkey[0] = first.alias;
						joinkey[1] = "P";
						joinkey[2] = second.alias;
						joinkey[3] = "S";
						join.add(joinkey);
					}
					else if (entry1.getKey() == 1 && entry2.getKey() == 1){
						String[] joinkey = new String[4];
						joinkey[0] = first.alias;
						joinkey[1] = "P";
						joinkey[2] = second.alias;
						joinkey[3] = "P";
						join.add(joinkey);
					}
					else if (entry1.getKey() == 1 && entry2.getKey() == 2){
						String[] joinkey = new String[4];
						joinkey[0] = first.alias;
						joinkey[1] = "P";
						joinkey[2] = second.alias;
						joinkey[3] = "O";
						join.add(joinkey);
					}
					else if (entry1.getKey() == 2 && entry2.getKey() == 0){
						String[] joinkey = new String[4];
						joinkey[0] = first.alias;
						joinkey[1] = "O";
						joinkey[2] = second.alias;
						joinkey[3] = "S";
						join.add(joinkey);
					}
					else if (entry1.getKey() == 2 && entry2.getKey() == 1){
						String[] joinkey = new String[4];
						joinkey[0] = first.alias;
						joinkey[1] = "O";
						joinkey[2] = second.alias;
						joinkey[3] = "P";
						join.add(joinkey);
					}
					else if (entry1.getKey() == 2 && entry2.getKey() == 2){
						String[] joinkey = new String[4];
						joinkey[0] = first.alias;
						joinkey[1] = "O";
						joinkey[2] = second.alias;
						joinkey[3] = "O";
						join.add(joinkey);
					}
					result = true;
				}
			}
		}
		return result;
	}

	/**
	 * 处理Filters 为了找出Sparql中的所有过滤条件并单独提取出来，
	 * 假如存在可以用分区的情况，则添加分区选择
	 */
	public void processFilters(){
		for (QGNode node : nodes.values()) {
			String table = node.alias;
			boolean hasPKey = false;//一次只能有一个分区选择
			HashMap<String, String> keys = new HashMap<String, String>();
			if (node.constSubject) {
				keys.put("S", node.subject);
				createPKey(keys, node.subject, "ok");
			}
			if (node.constPredicate) {
				keys.put("P", node.predicate);
			}
			if (node.constObject && !hasPKey) {
				keys.put("O", node.object);
				createPKey(keys, node.object, "ok");
			}

			if(keys.size() != 0){
				filterKeys.put(table, keys);
			}
		}

//		// 寻找可能的选择分区方案
//		if(edges.size() != 0){
//			for(QGEdge edge : edges.values()){
//
//				String left = edge.first.alias;
//				String right = edge.second.alias;
//				List<String[]> joins = edge.joins;
//
//				String lkey = joins.get(0)[1];
//				String rkey = joins.get(0)[3];
//
//				if(lkey.equalsIgnoreCase("P")&&rkey.equalsIgnoreCase("P"))
//					continue;
//
//				HashMap<String, String> flkeys = filterKeys.get(left);
//				HashMap<String, String> frkeys = filterKeys.get(right);
//
//				boolean ll = false;
//				boolean rr = false;
//				if(flkeys == null){
//					flkeys = new HashMap<String, String>();
//					ll = true;
//				}
//				if(frkeys == null){
//					frkeys = new HashMap<String, String>();
//					rr = true;
//				}
//
//				if(!findPKey(left)){
//					String val = frkeys.get(rkey);
//					if(val != null){
//						if(lkey == rkey){
//							// 如果连接键相同，需要相同的表才能操作
//							if(!edge.first.tableName.equalsIgnoreCase(edge.second.tableName)){
//								edge.first.tableName = edge.second.tableName;
//								super.setTabAlias(left, edge.first.tableName);
//							}
//						}
//						createPKey(flkeys, val, "OK");
//					}
//				}
//
//				if(!findPKey(right)){
//					String val = flkeys.get(lkey);
//					if(val != null){
//						if(lkey == rkey){
//							// 如果连接键相同，需要相同的表才能操作
//							if(!edge.first.tableName.equalsIgnoreCase(edge.second.tableName)){
//								edge.second.tableName = edge.first.tableName;
//								super.setTabAlias(right, edge.second.tableName);
//							}
//						}
//						createPKey(frkeys, val, "OK");
//					}
//				}
//
//				if(ll){
//					filterKeys.put(left, flkeys);
//				}
//				if(rr){
//					filterKeys.put(right, frkeys);
//				}
//
//			}
//		}
	}
	/**
	 * 创建分区过滤条件
	 *
	 */
	public void createPKey(HashMap<String, String> keys, String so, String psoKey){
		// 分区表
		Long number = null;
		try {
			number = Long.valueOf(so);
			long digi = QueryGraph.digi;
			long a = (long) (number / digi);
			long b = number % digi;
			if (b != 0) {
        number = digi * (a + 1);
      }
			keys.put(psoKey, String.valueOf(number));
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
		}
	}

	public boolean findPKey(String table){

		HashMap<String, String> keys = filterKeys.get(table);

		if(keys == null) {
      return false;
    }

		if(keys.get("SK") != null || keys.get("OK") != null) {
      return true;
    }

		return false;
	}

	public HashMap<String, HashMap<String, String>> getFilterKeys(){
		return filterKeys;
	}

	public HashMap<Integer, String> getAlias() {
		return aliases;
	}

	/**
	 * @return the aliasToTable
	 */
	public HashMap<String, Table> getAliasToTable() {
		return aliasToTable;
	}

	/**
	 * @param aliasToTable
	 *            the aliasToTable to set
	 */
	public void setAliasToTable(String alias, Table tb) {
		aliasToTable.put(alias, tb);
	}

	/**
	 * @return the projectionsNums
	 */
	public int getProjectionsNums() {
		return projectionsNums;
	}

	/**
	 * @return the nodesNums
	 */
	public int getNodesNums() {
		return nodesNums;
	}

	/**
	 * @return the edgesNums
	 */
	public int getEdgesNums() {
		return edgesNums;
	}

	/**
	 * @return the nodes
	 */
	public HashMap<Integer, QGNode> getNodes() {
		return nodes;
	}

	/**
	 * @return the edges
	 */
	public HashMap<Integer, QGEdge> getEdges() {
		return edges;
	}

	/**
	 * @return the projections
	 */
	public HashMap<Integer, String> getProjections() {
		return projections;
	}

	/**
	 * @param nodes
	 *            the nodes to set
	 */
	public void setNodes(HashMap<Integer, QGNode> nodes) {
		this.nodes = nodes;
	}

	/**
	 * @param edges
	 *            the edges to set
	 */
	public void setEdges(HashMap<Integer, QGEdge> edges) {
		this.edges = edges;
	}

	/**
	 * @param projections
	 *            the projections to set
	 */
	public void setProjections(HashMap<Integer, String> projections) {
		this.projections = projections;
	}

	/**
	 * @return the tablename
	 */
	public static String getTablename() {
		return tableName;
	}

	// public SparqlJoinTree getJoinTree() {
	// return joinTree;
	// }
	//
	// public void setJoinTree(SparqlJoinTree joinTree) {
	// this.joinTree = joinTree;
	// }

	public HashMap<Integer, String> getProjectionToTable() {
		return projectionToTable;
	}

	public void setProjectionToTable(HashMap<Integer, String> projectionToTable) {
		this.projectionToTable = projectionToTable;
	}

	public HashMap<Integer, String> getProjectionToAlias() {
		return projectionToAlias;
	}

	public void setProjectionToAlias(HashMap<Integer, String> projectionToAlias) {
		this.projectionToAlias = projectionToAlias;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
