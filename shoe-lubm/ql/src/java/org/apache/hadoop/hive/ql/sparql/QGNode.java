package org.apache.hadoop.hive.ql.sparql;

import java.util.HashMap;

/**
 * @作者 张仲伟
 * @时间 2012-8-28 上午11:22:20
 * @项目 Sparql
 * @联系方式 wai1316@qq.com
 */
public class QGNode {

	// private int isubject;
	// private int ipredicate;
	// private int iobject;

	public String subject;
	public String predicate;
	public String object;

	public String alias;
	public String tableName;

	public boolean constSubject;
	public boolean constPredicate;
	public boolean constObject;

	public HashMap<String, String> varToSPO;

	public int nodeId = -1;

	public QGNode(String subject, String predicate, String object,
			boolean constSubject, boolean constPredicate, boolean constObject) {
		// TODO Auto-generated constructor stub
		this.subject = subject;
		this.predicate = predicate;
		this.object = object;
		this.constSubject = constSubject;
		this.constPredicate = constPredicate;
		this.constObject = constObject;

		varToSPO = new HashMap<String, String>();

		if (!constSubject) {
      varToSPO.put(subject, "S");
    }
		if (!constPredicate) {
      varToSPO.put(predicate, "P");
    }
		if (!constObject) {
      varToSPO.put(object, "O");
    }

		if(constSubject){
			tableName = QueryGraph.stableName;
		}else{
			tableName = QueryGraph.otableName;
		}

	}

	/**
	 * @return the nodeId
	 */
	public int getNodeId() {
		return nodeId;
	}

	/**
	 * @param nodeId
	 *            the nodeId to set
	 */
	public void setNodeId(int nodeId) {
		this.nodeId = nodeId;
	}

	public String getAlias() {
		return alias;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}

}
