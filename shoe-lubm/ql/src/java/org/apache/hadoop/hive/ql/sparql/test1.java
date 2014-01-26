package org.apache.hadoop.hive.ql.sparql;

import java.io.IOException;

import org.antlr.runtime.RecognitionException;



public class test1 {

	private static class A {
		int aa;
	}

	public A inita()
	{
		A a = new A();
		a.aa = 1;
		return a;
	}

	public void function(A a){
		System.out.println(a.aa);
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws RecognitionException
	 */
	public static void main(String[] args) throws IOException, RecognitionException {
		// TODO Auto-generated method stub
//		ANTLRInputStream input = new ANTLRInputStream(System.in);
//		SparqlLexer lexer = new SparqlLexer(input);
//		CommonTokenStream tokens = new CommonTokenStream(lexer);
//		SparqlParser parser = new SparqlParser(tokens);
//		SparqlParser.query_return r = parser.query();
//		System.out.println(((BaseTree)r.getTree()).getChildCount());
//		System.out.println(((BaseTree)r.getTree()).toStringTree());
		test1 t = new test1();
		t.function(t.inita());

	}

}
