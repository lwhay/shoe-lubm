package org.apache.hadoop.hive.ql.sparql;

import java.io.IOException;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.Token;
import org.antlr.runtime.TokenRewriteStream;
import org.antlr.runtime.tree.CommonTreeAdaptor;
import org.antlr.runtime.tree.TreeAdaptor;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.io.rcfile.merge.BlockMergeTask;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.sparql.parser.SparqlLexer;
import org.apache.hadoop.hive.ql.sparql.parser.SparqlParser;
import org.apache.hadoop.mapred.JobConf;
/**
 * @作者 张仲伟
 * @时间 2012-8-28 上午11:22:20
 * @项目 Sparql
 * @联系方式 wai1316@qq.com
 */
public class SparqlQuery {

	private final HiveConf conf;
	private Context ctx;

	public SparqlQuery() {
		conf = null;
	}

	public SparqlQuery(HiveConf conf) {
		// TODO Auto-generated constructor stub
		this.conf = conf;
	}

	/**
	 * Tree adaptor for making antlr return ASTNodes instead of CommonTree nodes
	 * so that the graph walking algorithms and the rules framework defined in
	 * ql.lib can be used with the AST Nodes.
	 */
	static final TreeAdaptor adaptor = new CommonTreeAdaptor() {
		@Override
		public Object create(Token payload) {
			return new ASTNode(payload);
		}
	};

	public void run(String command) throws SemanticException, IOException {

		// 创建语法树，为ASTNode对象
		SparqlLexer lexer = new SparqlLexer(new ANTLRStringStream(command));
		TokenRewriteStream tokens = new TokenRewriteStream(lexer);

		SparqlParser parser = new SparqlParser(tokens);
		parser.setTreeAdaptor(adaptor);
		SparqlParser.query_return r = null;
		try {
			r = parser.query();
		} catch (RecognitionException e) {
			System.err.println(e);
		}

		ASTNode tree = (ASTNode) r.getTree();

		// 语法分析
		ctx = new Context(conf);
		ctx.setTryCount(Integer.MAX_VALUE);
		ctx.setCmd(command);
		ctx.setHDFSCleanup(true);

		run(tree, ctx);
	}

	public void run(ASTNode ast, Context ctx) throws SemanticException,
			IOException {

		SparqlSemanticAnalyzer sem = new SparqlSemanticAnalyzer(conf);
		sem.analyze(ast, ctx);

	}

	public ASTNode parse(String command) throws ParseException {
		return parse(command, null);
	}

	/**
	 * Parses a command, optionally assigning the parser's token stream to the
	 * given context.
	 *
	 * @param command
	 *            command to parse
	 *
	 * @param ctx
	 *            context with which to associate this parser's token stream, or
	 *            null if either no context is available or the context already
	 *            has an existing stream
	 *
	 * @return parsed AST
	 */
	public ASTNode parse(String command, Context ctx) throws ParseException {

		// 创建语法树，为ASTNode对象
		SparqlLexer lexer = new SparqlLexer(new ANTLRStringStream(command));
		TokenRewriteStream tokens = new TokenRewriteStream(lexer);
		if (ctx != null) {
			ctx.setTokenRewriteStream(tokens);
		}
		SparqlParser parser = new SparqlParser(tokens);
		parser.setTreeAdaptor(adaptor);
		SparqlParser.query_return r = null;
		try {
			r = parser.query();
		} catch (RecognitionException e) {
			System.err.println(e);
		}

		return (ASTNode) r.getTree();
	}

	/**
	 * @param args
	 * @throws SemanticException
	 * @throws IOException
	 */
	public static void main(String[] args) throws SemanticException,
			IOException {
	    JobConf conf = new JobConf(BlockMergeTask.class);
	      conf.addResource(new Path("hive-site.xml"));
	    HiveConf hiveConf = new HiveConf(conf, BlockMergeTask.class);
		// TODO Auto-generated method stub
		SparqlQuery query = new SparqlQuery(hiveConf);
		query.run("select ?a where{?a ?b \"sadf\".?a ?c \"a\"}");
	}

}
