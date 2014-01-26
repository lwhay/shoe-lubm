package org.apache.hadoop.hive.ql.sparql;
import org.apache.hadoop.hive.ql.exec.UDF;

public class hashTest extends UDF{

	public long hash(long key){


		return key % 64;
	}




	public String evaluate(){
	     return "hello world!";
	}

	public String evaluate(String str){
	     return "hello world: " + str;
	}




	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
