package org.apache.hadoop.hive.ql.sparql;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryProperties;
import org.apache.hadoop.hive.ql.exec.AbstractMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.ColumnStatsTask;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.ExecDriver;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.MapRedTask;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.RecordReader;
import org.apache.hadoop.hive.ql.exec.RecordWriter;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.StatsTask;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.metadata.DummyPartition;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.GenMRFileSink1;
import org.apache.hadoop.hive.ql.optimizer.GenMROperator;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext.GenMapRedCtx;
import org.apache.hadoop.hive.ql.optimizer.GenMRRedSink1;
import org.apache.hadoop.hive.ql.optimizer.GenMRRedSink2;
import org.apache.hadoop.hive.ql.optimizer.GenMRRedSink3;
import org.apache.hadoop.hive.ql.optimizer.GenMRRedSink4;
import org.apache.hadoop.hive.ql.optimizer.GenMRTableScan1;
import org.apache.hadoop.hive.ql.optimizer.GenMRUnion1;
import org.apache.hadoop.hive.ql.optimizer.GenMapRedUtils;
import org.apache.hadoop.hive.ql.optimizer.MapJoinFactory;
import org.apache.hadoop.hive.ql.optimizer.Optimizer;
import org.apache.hadoop.hive.ql.optimizer.physical.PhysicalContext;
import org.apache.hadoop.hive.ql.optimizer.physical.PhysicalOptimizer;
import org.apache.hadoop.hive.ql.optimizer.unionproc.UnionProcContext;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.tableSpec.SpecType;
import org.apache.hadoop.hive.ql.parse.GenMapRedWalker;
import org.apache.hadoop.hive.ql.parse.GlobalLimitCtx;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.IndexUpdater;
import org.apache.hadoop.hive.ql.parse.JoinCond;
import org.apache.hadoop.hive.ql.parse.JoinType;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.QBExpr;
import org.apache.hadoop.hive.ql.parse.QBJoinTree;
import org.apache.hadoop.hive.ql.parse.QBMetaData;
import org.apache.hadoop.hive.ql.parse.QBParseInfo;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.SplitSample;
import org.apache.hadoop.hive.ql.parse.TableAccessAnalyzer;
import org.apache.hadoop.hive.ql.plan.ColumnStatsDesc;
import org.apache.hadoop.hive.ql.plan.ColumnStatsWork;
import org.apache.hadoop.hive.ql.plan.CreateTableDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeNullDesc;
import org.apache.hadoop.hive.ql.plan.ExtractDesc;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc.sampleDesc;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.ListBucketingCtx;
import org.apache.hadoop.hive.ql.plan.LoadFileDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.ScriptDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.plan.UDTFDesc;
import org.apache.hadoop.hive.ql.plan.UnionDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.ResourceType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.InputFormat;

import org.apache.hadoop.hive.ql.sparql.parser.SparqlParser;

/**
 * @作者 张仲伟
 * @时间 2012-8-28 上午11:22:20
 * @项目 Sparql
 * @联系方式 wai1316@qq.com
 */
@SuppressWarnings("deprecation")
public class SparqlSemanticAnalyzer extends BaseSemanticAnalyzer {

	// private HiveConf conf;
	private HashMap<TableScanOperator, ExprNodeDesc> opToPartPruner;
	private HashMap<TableScanOperator, PrunedPartitionList> opToPartList;
	private HashMap<String, Operator<? extends OperatorDesc>> topOps;
	private HashMap<String, Operator<? extends OperatorDesc>> topSelOps;
	private LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext> opParseCtx;
	private List<LoadTableDesc> loadTableWork;
	private List<LoadFileDesc> loadFileWork;
	private Map<JoinOperator, QBJoinTree> joinContext;
	private final HashMap<TableScanOperator, Table> topToTable;
	private QueryGraph querygraph;
	private final List<String> projections;
	private ASTNode ast;
	private int destTableId;
	private UnionProcContext uCtx;
	List<AbstractMapJoinOperator<? extends MapJoinDesc>> listMapJoinOpsNoReducer;
	private HashMap<TableScanOperator, sampleDesc> opToSamplePruner;
	private final Map<TableScanOperator, Map<String, ExprNodeDesc>> opToPartToSkewedPruner;
	/**
	 * a map for the split sampling, from ailias to an instance of SplitSample
	 * that describes percentage and number.
	 */
	private final HashMap<String, SplitSample> nameToSplitSample;
	Map<GroupByOperator, Set<String>> groupOpToInputTables;
	Map<String, PrunedPartitionList> prunedPartitions;
	@SuppressWarnings("unused")
	private List<FieldSchema> resultSchema;
	// private CreateViewDesc createVwDesc;
	// private ASTNode viewSelect;
	private final UnparseTranslator unparseTranslator;
	private final GlobalLimitCtx globalLimitCtx = new GlobalLimitCtx();

	// prefix for column names auto generated by hive
	private final String autogenColAliasPrfxLbl;
	// Keep track of view alias to read entity corresponding to the view
	// For eg: for a query like 'select * from V3', where V3 -> V2, V2 -> V1, V1
	// -> T
	// keeps track of aliases for V3, V3:V2, V3:V2:V1.
	// This is used when T is added as an input for the query, the parents of T
	// is
	// derived from the alias V3:V2:V1:T
	private final Map<String, ReadEntity> viewAliasToInput = new HashMap<String, ReadEntity>();

	// Max characters when auto generating the column name with func name
	// private static final int AUTOGEN_COLALIAS_PRFX_MAXLENGTH = 20;

	public static final int JOINNODEDESC = 1;
	public static final int FILTERNODEDESC = 2;
	public static final int SELECTNODEDESC = 5;

	public SparqlSemanticAnalyzer(HiveConf conf) throws SemanticException {
		super(conf);
		// this.conf = conf;
		projections = new LinkedList<String>();
		opToPartPruner = new HashMap<TableScanOperator, ExprNodeDesc>();
		opToPartList = new HashMap<TableScanOperator, PrunedPartitionList>();
		opToSamplePruner = new HashMap<TableScanOperator, sampleDesc>();
		nameToSplitSample = new HashMap<String, SplitSample>();
		topOps = new HashMap<String, Operator<? extends OperatorDesc>>();
		topSelOps = new HashMap<String, Operator<? extends OperatorDesc>>();
		loadTableWork = new ArrayList<LoadTableDesc>();
		loadFileWork = new ArrayList<LoadFileDesc>();
		opParseCtx = new LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext>();
		joinContext = new HashMap<JoinOperator, QBJoinTree>();
		topToTable = new HashMap<TableScanOperator, Table>();
		destTableId = 1;
		uCtx = null;
		listMapJoinOpsNoReducer = new ArrayList<AbstractMapJoinOperator<? extends MapJoinDesc>>();
		groupOpToInputTables = new HashMap<GroupByOperator, Set<String>>();
		prunedPartitions = new HashMap<String, PrunedPartitionList>();
		unparseTranslator = new UnparseTranslator();
		autogenColAliasPrfxLbl = HiveConf.getVar(conf,
			HiveConf.ConfVars.HIVE_AUTOGEN_COLUMNALIAS_PREFIX_LABEL);
		HiveConf.getBoolVar(conf,
			HiveConf.ConfVars.HIVE_AUTOGEN_COLUMNALIAS_PREFIX_INCLUDEFUNCNAME);
		queryProperties = new QueryProperties();
		opToPartToSkewedPruner = new HashMap<TableScanOperator, Map<String, ExprNodeDesc>>();

	}

	private static class Phase1Ctx {
		String dest;
		int nextNum;
	}

	public Phase1Ctx initPhase1Ctx() {
		Phase1Ctx ctx_1 = new Phase1Ctx();
		ctx_1.nextNum = 0;
		ctx_1.dest = "reduce";
		return ctx_1;
	}

	private void print(ASTNode ast) {
		List<Node> ls = ast.getChildren();
		for (int i = 0; i < ls.size(); i++) {
			System.out.println(((ASTNode)ls).getToken().toString());
			print((ASTNode)ls.get(i));
		}
	}

	@Override
  @SuppressWarnings("rawtypes")
	public void analyzeInternal(ASTNode ast) throws SemanticException {

		this.ast = ast;
		new ArrayList<String>();

		QueryGraph querygraph = new QueryGraph();
		this.querygraph = querygraph;
		LOG.info("Starting Semantic Analysis");

		// analyze and process the position alias
		// processPositionAlias(ast);

		// analyze create table command
		// if (ast.getToken().getType() == HiveParser.TOK_CREATETABLE) {
		// // if it is not CTAS, we don't need to go further and just return
		// if ((child = analyzeCreateTable(ast, qb)) == null) {
		// return;
		// }
		// } else {
		SessionState.get().setCommandType(HiveOperation.QUERY);
		// }

		// analyze create view command
		// if (ast.getToken().getType() == HiveParser.TOK_CREATEVIEW ||
		// ast.getToken().getType() == HiveParser.TOK_ALTERVIEW_AS) {
		// child = analyzeCreateView(ast, qb);
		// SessionState.get().setCommandType(HiveOperation.CREATEVIEW);
		// if (child == null) {
		// return;
		// }
		// viewSelect = child;
		// // prevent view from referencing itself
		// viewsExpanded.add(db.getCurrentDatabase() + "." +
		// createVwDesc.getViewName());
		// }

		// continue analyzing from the child ASTNode.
		if (!doPhase1(ast, querygraph)) {
			// if phase1Result false return
			return;
		}
		System.out.println("****************************************************");
	  //print(ast);
		System.out.println("****************************************************");
		//System.out.println(ast.);
		LOG.info("Completed phase 1 of Semantic Analysis");

		getMetaData(querygraph);
		LOG.info("Completed getting MetaData in Semantic Analysis");

		// Save the result schema derived from the sink operator produced
		// by genPlan. This has the correct column names, which clients
		// such as JDBC would prefer instead of the c0, c1 we'll end
		// up with later.
		Operator sinkOp = genPlan(querygraph);

		resultSchema = convertRowSchemaToViewSchema(opParseCtx.get(sinkOp)
				.getRowResolver());

		// if (createVwDesc != null) {
		// saveViewDefinition();
		//
		// // validate the create view statement
		// // at this point, the createVwDesc gets all the information for
		// // semantic check
		// validateCreateView(createVwDesc);
		//
		// // Since we're only creating a view (not executing it), we
		// // don't need to optimize or translate the plan (and in fact, those
		// // procedures can interfere with the view creation). So
		// // skip the rest of this method.
		// ctx.setResDir(null);
		// ctx.setResFile(null);
		// return;
		// }
		ParseContext pCtx = new ParseContext(conf, querygraph, ast,
				opToPartPruner, opToPartList, topOps, topSelOps, opParseCtx,
				joinContext, topToTable, loadTableWork, loadFileWork, ctx,
				idToTableNameMap, destTableId, uCtx, listMapJoinOpsNoReducer,
				groupOpToInputTables, prunedPartitions, opToSamplePruner,
				globalLimitCtx, nameToSplitSample, inputs, rootTasks,
				opToPartToSkewedPruner, viewAliasToInput);

		// Generate table access stats if required
		if (HiveConf.getBoolVar(this.conf,
			HiveConf.ConfVars.HIVE_STATS_COLLECT_TABLEKEYS) == true) {
			TableAccessAnalyzer tableAccessAnalyzer = new TableAccessAnalyzer(
					pCtx);
			setTableAccessInfo(tableAccessAnalyzer.analyzeTableAccess());
		}

		Optimizer optm = new Optimizer();
		optm.setPctx(pCtx);
		optm.initialize(conf);
		pCtx = optm.optimize();

		// At this point we have the complete operator tree
		// from which we want to find the reduce operator
		genMapRedTasks(pCtx);

		LOG.info("Completed plan generation");

		return;
	}

	private List<FieldSchema> convertRowSchemaToViewSchema(RowResolver rr) {
		List<FieldSchema> fieldSchemas = new ArrayList<FieldSchema>();
		for (ColumnInfo colInfo : rr.getColumnInfos()) {
			if (colInfo.isHiddenVirtualCol()) {
				continue;
			}
			String colName = rr.reverseLookup(colInfo.getInternalName())[1];
			fieldSchemas.add(new FieldSchema(colName, colInfo.getType()
					.getTypeName(), null));
		}
		return fieldSchemas;
	}

	public void initParseCtx(ParseContext pctx) {
		opToPartPruner = pctx.getOpToPartPruner();
		opToPartList = pctx.getOpToPartList();
		opToSamplePruner = pctx.getOpToSamplePruner();
		topOps = pctx.getTopOps();
		topSelOps = pctx.getTopSelOps();
		opParseCtx = pctx.getOpParseCtx();
		loadTableWork = pctx.getLoadTableWork();
		loadFileWork = pctx.getLoadFileWork();
		joinContext = pctx.getJoinContext();
		ctx = pctx.getContext();
		destTableId = pctx.getDestTableId();
		idToTableNameMap = pctx.getIdToTableNameMap();
		uCtx = pctx.getUCtx();
		listMapJoinOpsNoReducer = pctx.getListMapJoinOpsNoReducer();
		querygraph = (QueryGraph) pctx.getQB();
		groupOpToInputTables = pctx.getGroupOpToInputTables();
		prunedPartitions = pctx.getPrunedPartitions();
		fetchTask = pctx.getFetchTask();
		setLineageInfo(pctx.getLineageInfo());
	}

	@SuppressWarnings({ "nls", "unchecked" })
	private void genMapRedTasks(ParseContext pCtx) throws SemanticException {
		boolean isCStats = querygraph.isAnalyzeRewrite();

		if (pCtx.getFetchTask() != null) {
			// replaced by single fetch task
			initParseCtx(pCtx);
			return;
		}

		initParseCtx(pCtx);
		List<Task<MoveWork>> mvTask = new ArrayList<Task<MoveWork>>();

		/*
		 * In case of a select, use a fetch task instead of a move task. If the
		 * select is from analyze table column rewrite, don't create a fetch
		 * task. Instead create a column stats task later.
		 */
		if (querygraph.getIsQuery() && !isCStats) {
			if ((!loadTableWork.isEmpty()) || (loadFileWork.size() != 1)) {
				throw new SemanticException(ErrorMsg.GENERIC_ERROR.getMsg());
			}
			String cols = loadFileWork.get(0).getColumns();
			String colTypes = loadFileWork.get(0).getColumnTypes();

			String resFileFormat = HiveConf.getVar(conf,
				HiveConf.ConfVars.HIVEQUERYRESULTFILEFORMAT);
			TableDesc resultTab = PlanUtils.getDefaultQueryOutputTableDesc(
				cols, colTypes, resFileFormat);

			FetchWork fetch = new FetchWork(new Path(loadFileWork.get(0)
					.getSourceDir()).toString(), resultTab, querygraph
					.getParseInfo().getOuterQueryLimit());

			FetchTask fetchTask = (FetchTask) TaskFactory.get(fetch, conf);
			setFetchTask(fetchTask);

			// For the FetchTask, the limit optimiztion requires we fetch all
			// the rows
			// in memory and count how many rows we get. It's not practical if
			// the
			// limit factor is too big
			int fetchLimit = HiveConf.getIntVar(conf,
				HiveConf.ConfVars.HIVELIMITOPTMAXFETCH);
			if (globalLimitCtx.isEnable()
					&& globalLimitCtx.getGlobalLimit() > fetchLimit) {
				LOG.info("For FetchTask, LIMIT "
						+ globalLimitCtx.getGlobalLimit() + " > " + fetchLimit
						+ ". Doesn't qualify limit optimiztion.");
				globalLimitCtx.disableOpt();
			}
		} else if (!isCStats) {
			for (LoadTableDesc ltd : loadTableWork) {
				Task<MoveWork> tsk = TaskFactory.get(new MoveWork(null, null,
						ltd, null, false), conf);
				mvTask.add(tsk);
				// Check to see if we are stale'ing any indexes and auto-update
				// them if we want
				if (HiveConf.getBoolVar(conf,
					HiveConf.ConfVars.HIVEINDEXAUTOUPDATE)) {
					IndexUpdater indexUpdater = new IndexUpdater(loadTableWork,
							getInputs(), conf);
					try {
						List<Task<? extends Serializable>> indexUpdateTasks = indexUpdater
								.generateUpdateTasks();
						for (Task<? extends Serializable> updateTask : indexUpdateTasks) {
							tsk.addDependentTask(updateTask);
						}
					} catch (HiveException e) {
						console.printInfo("WARNING: could not auto-update stale indexes, which are not in sync");
					}
				}
			}

			boolean oneLoadFile = true;
			for (LoadFileDesc lfd : loadFileWork) {
				if (querygraph.isCTAS()) {
					assert (oneLoadFile); // should not have more than 1 load
											// file for
					// CTAS
					// make the movetask's destination directory the table's
					// destination.
					String location = querygraph.getTableDesc().getLocation();
					if (location == null) {
						// get the table's default location
						Table dumpTable;
						Path targetPath;
						try {
							dumpTable = db.newTable(querygraph.getTableDesc()
									.getTableName());
							if (!db.databaseExists(dumpTable.getDbName())) {
								throw new SemanticException(
										"ERROR: The database "
												+ dumpTable.getDbName()
												+ " does not exist.");
							}
							Warehouse wh = new Warehouse(conf);
							targetPath = wh.getTablePath(
								db.getDatabase(dumpTable.getDbName()),
								dumpTable.getTableName());
						} catch (HiveException e) {
							throw new SemanticException(e);
						} catch (MetaException e) {
							throw new SemanticException(e);
						}

						location = targetPath.toString();
					}
					lfd.setTargetDir(location);

					oneLoadFile = false;
				}
				mvTask.add(TaskFactory.get(new MoveWork(null, null, null, lfd,
						false), conf));
			}
		}

		// generate map reduce plans
		ParseContext tempParseContext = getParseContext();
		GenMRProcContext procCtx = new GenMRProcContext(
				conf,
				new HashMap<Operator<? extends OperatorDesc>, Task<? extends Serializable>>(),
				new ArrayList<Operator<? extends OperatorDesc>>(),
				tempParseContext,
				mvTask,
				rootTasks,
				new LinkedHashMap<Operator<? extends OperatorDesc>, GenMapRedCtx>(),
				inputs, outputs);

		// create a walker which walks the tree in a DFS manner while
		// maintaining
		// the operator stack.
		// The dispatcher generates the plan from the operator tree
		Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
		opRules.put(
			new RuleRegExp(new String("R1"), TableScanOperator
					.getOperatorName() + "%"), new GenMRTableScan1());
		opRules.put(
			new RuleRegExp(new String("R2"), TableScanOperator
					.getOperatorName()
					+ "%.*"
					+ ReduceSinkOperator.getOperatorName() + "%"),
			new GenMRRedSink1());
		opRules.put(
			new RuleRegExp(new String("R3"), ReduceSinkOperator
					.getOperatorName()
					+ "%.*"
					+ ReduceSinkOperator.getOperatorName() + "%"),
			new GenMRRedSink2());
		opRules.put(
			new RuleRegExp(new String("R4"), FileSinkOperator.getOperatorName()
					+ "%"), new GenMRFileSink1());
		opRules.put(
			new RuleRegExp(new String("R5"), UnionOperator.getOperatorName()
					+ "%"), new GenMRUnion1());
		opRules.put(
			new RuleRegExp(new String("R6"), UnionOperator.getOperatorName()
					+ "%.*" + ReduceSinkOperator.getOperatorName() + "%"),
			new GenMRRedSink3());
		opRules.put(
			new RuleRegExp(new String("R6"), MapJoinOperator.getOperatorName()
					+ "%.*" + ReduceSinkOperator.getOperatorName() + "%"),
			new GenMRRedSink4());
		opRules.put(
			new RuleRegExp(new String("R7"), TableScanOperator
					.getOperatorName()
					+ "%.*"
					+ MapJoinOperator.getOperatorName() + "%"), MapJoinFactory
					.getTableScanMapJoin());
		opRules.put(
			new RuleRegExp(new String("R8"), ReduceSinkOperator
					.getOperatorName()
					+ "%.*"
					+ MapJoinOperator.getOperatorName() + "%"), MapJoinFactory
					.getReduceSinkMapJoin());
		opRules.put(
			new RuleRegExp(new String("R9"), UnionOperator.getOperatorName()
					+ "%.*" + MapJoinOperator.getOperatorName() + "%"),
			MapJoinFactory.getUnionMapJoin());
		opRules.put(
			new RuleRegExp(new String("R10"), MapJoinOperator.getOperatorName()
					+ "%.*" + MapJoinOperator.getOperatorName() + "%"),
			MapJoinFactory.getMapJoinMapJoin());
		opRules.put(
			new RuleRegExp(new String("R11"), MapJoinOperator.getOperatorName()
					+ "%" + SelectOperator.getOperatorName() + "%"),
			MapJoinFactory.getMapJoin());

		// The dispatcher fires the processor corresponding to the closest
		// matching
		// rule and passes the context along
		Dispatcher disp = new DefaultRuleDispatcher(new GenMROperator(),
				opRules, procCtx);

		GraphWalker ogw = new GenMapRedWalker(disp);
		ArrayList<Node> topNodes = new ArrayList<Node>();
		topNodes.addAll(topOps.values());
		ogw.startWalking(topNodes, null);

		/*
		 * If the query was the result of analyze table column compute
		 * statistics rewrite, create a column stats task instead of a fetch
		 * task to persist stats to the metastore.
		 */
		if (isCStats) {
			genColumnStatsTask(querygraph);
		}

		// reduce sink does not have any kids - since the plan by now has been
		// broken up into multiple
		// tasks, iterate over all tasks.
		// For each task, go over all operators recursively
		for (Task<? extends Serializable> rootTask : rootTasks) {
			breakTaskTree(rootTask);
		}

		// For each task, set the key descriptor for the reducer
		for (Task<? extends Serializable> rootTask : rootTasks) {
			setKeyDescTaskTree(rootTask);
		}

		// If a task contains an operator which instructs
		// bucketizedhiveinputformat
		// to be used, please do so
		for (Task<? extends Serializable> rootTask : rootTasks) {
			setInputFormat(rootTask);
		}

		PhysicalContext physicalContext = new PhysicalContext(conf,
				getParseContext(), ctx, rootTasks, fetchTask);
		PhysicalOptimizer physicalOptimizer = new PhysicalOptimizer(
				physicalContext, conf);
		physicalOptimizer.optimize();

		// For each operator, generate the counters if needed
		if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVEJOBPROGRESS)) {
			for (Task<? extends Serializable> rootTask : rootTasks) {
				generateCountersTask(rootTask);
			}
		}

		decideExecMode(rootTasks, ctx, globalLimitCtx);

		if (querygraph.isCTAS()) {
			// generate a DDL task and make it a dependent task of the leaf
			CreateTableDesc crtTblDesc = querygraph.getTableDesc();

			crtTblDesc.validate();

			// Clear the output for CTAS since we don't need the output from the
			// mapredWork, the
			// DDLWork at the tail of the chain will have the output
			getOutputs().clear();

			Task<? extends Serializable> crtTblTask = TaskFactory.get(
				new DDLWork(getInputs(), getOutputs(), crtTblDesc), conf);

			// find all leaf tasks and make the DDLTask as a dependent task of
			// all of
			// them
			HashSet<Task<? extends Serializable>> leaves = new HashSet<Task<? extends Serializable>>();
			getLeafTasks(rootTasks, leaves);
			assert (leaves.size() > 0);
			for (Task<? extends Serializable> task : leaves) {
				if (task instanceof StatsTask) {
					// StatsTask require table to already exist
					for (Task<? extends Serializable> parentOfStatsTask : task
							.getParentTasks()) {
						parentOfStatsTask.addDependentTask(crtTblTask);
					}
					for (Task<? extends Serializable> parentOfCrtTblTask : crtTblTask
							.getParentTasks()) {
						parentOfCrtTblTask.removeDependentTask(task);
					}
					crtTblTask.addDependentTask(task);
				} else {
					task.addDependentTask(crtTblTask);
				}
			}
		}

		if (globalLimitCtx.isEnable() && fetchTask != null) {
			LOG.info("set least row check for FetchTask: "
					+ globalLimitCtx.getGlobalLimit());
			fetchTask.getWork()
					.setLeastNumRows(globalLimitCtx.getGlobalLimit());
		}

		if (globalLimitCtx.isEnable()
				&& globalLimitCtx.getLastReduceLimitDesc() != null) {
			LOG.info("set least row check for LimitDesc: "
					+ globalLimitCtx.getGlobalLimit());
			globalLimitCtx.getLastReduceLimitDesc().setLeastRows(
				globalLimitCtx.getGlobalLimit());
			List<ExecDriver> mrTasks = Utilities.getMRTasks(rootTasks);
			for (ExecDriver tsk : mrTasks) {
				tsk.setRetryCmdWhenFail(true);
			}
		}
	}

	/**
	 * Find all leaf tasks of the list of root tasks.
	 */
	private void getLeafTasks(List<Task<? extends Serializable>> rootTasks,
			HashSet<Task<? extends Serializable>> leaves) {

		for (Task<? extends Serializable> root : rootTasks) {
			getLeafTasks(root, leaves);
		}
	}

	private void getLeafTasks(Task<? extends Serializable> task,
			HashSet<Task<? extends Serializable>> leaves) {
		if (task.getDependentTasks() == null) {
			if (!leaves.contains(task)) {
				leaves.add(task);
			}
		} else {
			getLeafTasks(task.getDependentTasks(), leaves);
		}
	}

	private void decideExecMode(List<Task<? extends Serializable>> rootTasks,
			Context ctx, GlobalLimitCtx globalLimitCtx)
			throws SemanticException {

		// bypass for explain queries for now
		if (ctx.getExplain()) {
			return;
		}

		// user has told us to run in local mode or doesn't want auto-local mode
		if (ctx.isLocalOnlyExecutionMode()
				|| !conf.getBoolVar(HiveConf.ConfVars.LOCALMODEAUTO)) {
			return;
		}

		final Context lCtx = ctx;
		PathFilter p = new PathFilter() {
			public boolean accept(Path file) {
				return !lCtx.isMRTmpFileURI(file.toUri().getPath());
			}
		};
		List<ExecDriver> mrtasks = Utilities.getMRTasks(rootTasks);

		// map-reduce jobs will be run locally based on data size
		// first find out if any of the jobs needs to run non-locally
		boolean hasNonLocalJob = false;
		for (ExecDriver mrtask : mrtasks) {
			try {
				ContentSummary inputSummary = Utilities.getInputSummary(ctx,
					(MapredWork) mrtask.getWork(), p);
				int numReducers = getNumberOfReducers(mrtask.getWork(), conf);

				long estimatedInput;

				if (globalLimitCtx != null && globalLimitCtx.isEnable()) {
					// If the global limit optimization is triggered, we will
					// estimate input data actually needed based on limit rows.
					// estimated Input = (num_limit * max_size_per_row) *
					// (estimated_map + 2)
					//
					long sizePerRow = HiveConf.getLongVar(conf,
						HiveConf.ConfVars.HIVELIMITMAXROWSIZE);
					estimatedInput = globalLimitCtx.getGlobalLimit()
							* sizePerRow;
					long minSplitSize = HiveConf.getLongVar(conf,
						HiveConf.ConfVars.MAPREDMINSPLITSIZE);
					long estimatedNumMap = inputSummary.getLength()
							/ minSplitSize + 1;
					estimatedInput = estimatedInput * (estimatedNumMap + 1);
				} else {
					estimatedInput = inputSummary.getLength();
				}

				if (LOG.isDebugEnabled()) {
					LOG.debug("Task: " + mrtask.getId() + ", Summary: "
							+ inputSummary.getLength() + ","
							+ inputSummary.getFileCount() + "," + numReducers
							+ ", estimated Input: " + estimatedInput);
				}

				if (MapRedTask.isEligibleForLocalMode(conf, numReducers,
					estimatedInput, inputSummary.getFileCount()) != null) {
					hasNonLocalJob = true;
					break;
				} else {
					mrtask.setLocalMode(true);
				}
			} catch (IOException e) {
				throw new SemanticException(e);
			}
		}

		if (!hasNonLocalJob) {
			// Entire query can be run locally.
			// Save the current tracker value and restore it when done.
			ctx.setOriginalTracker(ShimLoader.getHadoopShims()
					.getJobLauncherRpcAddress(conf));
			ShimLoader.getHadoopShims().setJobLauncherRpcAddress(conf, "local");
			console.printInfo("Automatically selecting local only mode for query");

			// If all the tasks can be run locally, we can use local disk for
			// storing intermediate data.

			/**
			 * This code is commented out pending further testing/development
			 * for (Task<? extends OperatorDesc> t: rootTasks)
			 * t.localizeMRTmpFiles(ctx);
			 */
		}
	}

	/**
	 * Make a best guess at trying to find the number of reducers
	 */
	private static int getNumberOfReducers(MapredWork mrwork, HiveConf conf) {
		if (mrwork.getReducer() == null) {
			return 0;
		}

		if (mrwork.getNumReduceTasks() >= 0) {
			return mrwork.getNumReduceTasks();
		}

		return conf.getIntVar(HiveConf.ConfVars.HADOOPNUMREDUCERS);
	}

	// loop over all the tasks recursviely
	private void generateCountersTask(Task<? extends Serializable> task) {
		if (task instanceof ExecDriver) {
			HashMap<String, Operator<? extends OperatorDesc>> opMap = ((MapredWork) task
					.getWork()).getAliasToWork();
			if (!opMap.isEmpty()) {
				for (Operator<? extends OperatorDesc> op : opMap.values()) {
					generateCountersOperator(op);
				}
			}

			Operator<? extends OperatorDesc> reducer = ((MapredWork) task
					.getWork()).getReducer();
			if (reducer != null) {
				LOG.info("Generating counters for operator " + reducer);
				generateCountersOperator(reducer);
			}
		} else if (task instanceof ConditionalTask) {
			List<Task<? extends Serializable>> listTasks = ((ConditionalTask) task)
					.getListTasks();
			for (Task<? extends Serializable> tsk : listTasks) {
				generateCountersTask(tsk);
			}
		}

		// Start the counters from scratch - a hack for hadoop 17.
		Operator.resetLastEnumUsed();

		if (task.getChildTasks() == null) {
			return;
		}

		for (Task<? extends Serializable> childTask : task.getChildTasks()) {
			generateCountersTask(childTask);
		}
	}

	private void generateCountersOperator(Operator<? extends OperatorDesc> op) {
		op.assignCounterNameToEnum();

		if (op.getChildOperators() == null) {
			return;
		}

		for (Operator<? extends OperatorDesc> child : op.getChildOperators()) {
			generateCountersOperator(child);
		}
	}

	private void setInputFormat(MapredWork work,
			Operator<? extends OperatorDesc> op) {
		if (op.isUseBucketizedHiveInputFormat()) {
			work.setUseBucketizedHiveInputFormat(true);
			return;
		}

		if (op.getChildOperators() != null) {
			for (Operator<? extends OperatorDesc> childOp : op
					.getChildOperators()) {
				setInputFormat(work, childOp);
			}
		}
	}

	// loop over all the tasks recursviely
	private void setInputFormat(Task<? extends Serializable> task) {
		if (task instanceof ExecDriver) {
			MapredWork work = (MapredWork) task.getWork();
			HashMap<String, Operator<? extends OperatorDesc>> opMap = work
					.getAliasToWork();
			if (!opMap.isEmpty()) {
				for (Operator<? extends OperatorDesc> op : opMap.values()) {
					setInputFormat(work, op);
				}
			}
		} else if (task instanceof ConditionalTask) {
			List<Task<? extends Serializable>> listTasks = ((ConditionalTask) task)
					.getListTasks();
			for (Task<? extends Serializable> tsk : listTasks) {
				setInputFormat(tsk);
			}
		}

		if (task.getChildTasks() != null) {
			for (Task<? extends Serializable> childTask : task.getChildTasks()) {
				setInputFormat(childTask);
			}
		}
	}

	// loop over all the tasks recursviely
	private void setKeyDescTaskTree(Task<? extends Serializable> task) {

		if (task instanceof ExecDriver) {
			MapredWork work = (MapredWork) task.getWork();
			work.deriveExplainAttributes();
			HashMap<String, Operator<? extends OperatorDesc>> opMap = work
					.getAliasToWork();
			if (!opMap.isEmpty()) {
				for (Operator<? extends OperatorDesc> op : opMap.values()) {
					GenMapRedUtils.setKeyAndValueDesc(work, op);
				}
			}
		} else if (task instanceof ConditionalTask) {
			List<Task<? extends Serializable>> listTasks = ((ConditionalTask) task)
					.getListTasks();
			for (Task<? extends Serializable> tsk : listTasks) {
				setKeyDescTaskTree(tsk);
			}
		}

		if (task.getChildTasks() == null) {
			return;
		}

		for (Task<? extends Serializable> childTask : task.getChildTasks()) {
			setKeyDescTaskTree(childTask);
		}
	}

	// loop over all the tasks recursviely
	private void breakTaskTree(Task<? extends Serializable> task) {

		if (task instanceof ExecDriver) {
			HashMap<String, Operator<? extends OperatorDesc>> opMap = ((MapredWork) task
					.getWork()).getAliasToWork();
			if (!opMap.isEmpty()) {
				for (Operator<? extends OperatorDesc> op : opMap.values()) {
					breakOperatorTree(op);
				}
			}
		} else if (task instanceof ConditionalTask) {
			List<Task<? extends Serializable>> listTasks = ((ConditionalTask) task)
					.getListTasks();
			for (Task<? extends Serializable> tsk : listTasks) {
				breakTaskTree(tsk);
			}
		}

		if (task.getChildTasks() == null) {
			return;
		}

		for (Task<? extends Serializable> childTask : task.getChildTasks()) {
			breakTaskTree(childTask);
		}
	}

	// loop over all the operators recursviely
	private void breakOperatorTree(Operator<? extends OperatorDesc> topOp) {
		if (topOp instanceof ReduceSinkOperator) {
			topOp.setChildOperators(null);
		}

		if (topOp.getChildOperators() == null) {
			return;
		}

		for (Operator<? extends OperatorDesc> op : topOp.getChildOperators()) {
			breakOperatorTree(op);
		}
	}

	/**
	 * A helper function to generate a column stats task on top of map-red task.
	 * The column stats task fetches from the output of the map-red task,
	 * constructs the column stats object and persists it to the metastore.
	 *
	 * This method generates a plan with a column stats task on top of map-red
	 * task and sets up the appropriate metadata to be used during execution.
	 *
	 * @param querygraph
	 */
	@SuppressWarnings("unchecked")
	private void genColumnStatsTask(QueryGraph querygraph) {
		QBParseInfo qbParseInfo = querygraph.getParseInfo();
		ColumnStatsTask cStatsTask = null;
		ColumnStatsWork cStatsWork = null;
		FetchWork fetch = null;
		String tableName = qbParseInfo.getTableName();
		String partName = qbParseInfo.getPartName();
		List<String> colName = qbParseInfo.getColName();
		List<String> colType = qbParseInfo.getColType();
		boolean isTblLevel = qbParseInfo.isTblLvl();

		String cols = loadFileWork.get(0).getColumns();
		String colTypes = loadFileWork.get(0).getColumnTypes();

		String resFileFormat = HiveConf.getVar(conf,
			HiveConf.ConfVars.HIVEQUERYRESULTFILEFORMAT);
		TableDesc resultTab = PlanUtils.getDefaultQueryOutputTableDesc(cols,
			colTypes, resFileFormat);

		fetch = new FetchWork(
				new Path(loadFileWork.get(0).getSourceDir()).toString(),
				resultTab, querygraph.getParseInfo().getOuterQueryLimit());

		ColumnStatsDesc cStatsDesc = new ColumnStatsDesc(tableName, partName,
				colName, colType, isTblLevel);
		cStatsWork = new ColumnStatsWork(fetch, cStatsDesc);
		cStatsTask = (ColumnStatsTask) TaskFactory.get(cStatsWork, conf);
		rootTasks.add(cStatsTask);
	}

	public ParseContext getParseContext() {
		return new ParseContext(conf, querygraph, ast, opToPartPruner,
				opToPartList, topOps, topSelOps, opParseCtx, joinContext,
				topToTable, loadTableWork, loadFileWork, ctx, idToTableNameMap,
				destTableId, uCtx, listMapJoinOpsNoReducer,
				groupOpToInputTables, prunedPartitions, opToSamplePruner,
				globalLimitCtx, nameToSplitSample, inputs, rootTasks,
				opToPartToSkewedPruner, viewAliasToInput);
	}

	public boolean doPhase1(ASTNode ast, QueryGraph querygraph)
			throws SemanticException {
		Phase1Ctx ctx_1 = initPhase1Ctx();
		ctx_1.dest = "insclause-" + ctx_1.nextNum;
		ctx_1.nextNum++;

		boolean res = doPhase1(ast, querygraph, ctx_1);

		querygraph.processProjections();
		return res;
	}

	/**
	 * 分析语法树，并提取树中各种信息分别保存起来
	 *
	 * @param ast
	 *            sparql语法树
	 * @param querygraph
	 *            保存树信息的结构
	 * @param ctx_1
	 */
	public boolean doPhase1(ASTNode ast, QueryGraph querygraph, Phase1Ctx ctx_1)
			throws SemanticException {

		boolean phase1Result = true;
		QBParseInfo qbp = querygraph.getParseInfo();
		boolean skipRecursion = false;

		if (ast.getToken() != null) {
			skipRecursion = true;
			switch (ast.getToken().getType()) {
			case SparqlParser.PROLOGUE: {
				break;
			}
			case SparqlParser.SELECT_CLAUSE: {
				qbp.setSelExprForClause(ctx_1.dest, ast);
				processSelect(ast, querygraph);
				break;
			}
			case SparqlParser.GROUP_GRAPH_PATTERN: {

				// 为了兼容性
				qbp.setDestForClause(ctx_1.dest, (ASTNode) ast);

				// TRIPLES_SAME_SUBJECT / UNION
				ASTNode parent = (ASTNode) ast.getChild(0);

				if (parent.getToken().getType() == SparqlParser.TRIPLES_SAME_SUBJECT) {

					// 在此设计中，sparql的where语句相当于HQL的from和where语句相结合
					// GROUP_GRAPH_PATTERN
					qbp.setWhrExprForClause(ctx_1.dest, ast);

					int child_count = ast.getChildCount();
					if (child_count > 1) {
						// 有多个TRIPLES_SAME_SUBJECT (optional)
						queryProperties.setHasJoin(true);
						qbp.setJoinExpr(ast);

					} else if (child_count == 1) {
						// TRIPLES_SAME_SUBJECT
						ASTNode child = (ASTNode) ast.getChild(0);
						int child_count2 = child.getChildCount();

						// 可能为SPOPO，即相同S的patterns在相同TRIPLES_SAME_SUBJECT里
						if (child_count2 > 3) {
							queryProperties.setHasJoin(true);
							qbp.setJoinExpr(ast);
						}
					}

					processWhere(ast, querygraph);
					querygraph.buildEdges();
					querygraph.processFilters();

				} else if (parent.getToken().getType() == SparqlParser.UNION) {
					// 处理连接
					processUnion(parent, querygraph, ctx_1);
				}
				break;
			}
			case SparqlParser.FROM: {
				processFrom(ast, querygraph);
				break;
			}
			default: {
				skipRecursion = false;
				break;
			}
			}
		}

		if (!skipRecursion) {
			// Iterate over the rest of the children
			int child_count = ast.getChildCount();
			for (int child_pos = 0; child_pos < child_count && phase1Result; ++child_pos) {
				// Recurse
				phase1Result = phase1Result
						&& doPhase1((ASTNode) ast.getChild(child_pos),
							querygraph, ctx_1);
			}
		}
		return phase1Result;
	}

	private void processUnion(ASTNode ast, QueryGraph querygraph,
			Phase1Ctx ctx_1) throws SemanticException {
		// TODO Auto-generated method stub
		if (ast.getChildCount() < 2) {
			throw new SemanticException(ErrorMsg.NO_SUBQUERY_ALIAS.getMsg(ast));
		}
		String alias = "tmp";

		// Recursively do the first phase of semantic analysis for the subquery
		QBExpr qbexpr = new QBExpr(alias);

		doPhase1QBExpr(ast, qbexpr, querygraph.getId(), alias, ctx_1);

		// If the alias is already there then we have a conflict
		if (querygraph.exists(alias)) {
			throw new SemanticException(
					ErrorMsg.AMBIGUOUS_TABLE_ALIAS.getMsg(ast.getChild(1)));
		}
		// Insert this map into the stats
		querygraph.setSubqAlias(alias, qbexpr);
		querygraph.addAlias(alias);

		unparseTranslator.addIdentifierTranslation((ASTNode) ast.getChild(1));
	}

	@SuppressWarnings("nls")
	public void doPhase1QBExpr(ASTNode ast, QBExpr qbexpr, String id,
			String alias, Phase1Ctx ctx_1) throws SemanticException {

		assert (ast.getToken() != null);
		switch (ast.getToken().getType()) {
		case SparqlParser.GROUP_GRAPH_PATTERN: {
			QueryGraph querygraph = new QueryGraph(id, alias, true);
			doPhase1(ast, querygraph, ctx_1);

			querygraph.getParseInfo().setSelExprForClause(ctx_1.dest, ast);
			for (int i = 0; i < projections.size(); i++) {
				querygraph.addProjection(projections.get(i));
			}
			querygraph.processProjections();

			qbexpr.setOpcode(QBExpr.Opcode.NULLOP);
			qbexpr.setQB(querygraph);
		}
			break;
		case SparqlParser.UNION: {
			qbexpr.setOpcode(QBExpr.Opcode.UNION);
			// query 1
			assert (ast.getChild(0) != null);
			QBExpr qbexpr1 = new QBExpr(alias + "-subquery1");
			doPhase1QBExpr((ASTNode) ast.getChild(0), qbexpr1, id
					+ "-subquery1", alias + "-subquery1", ctx_1);
			qbexpr.setQBExpr1(qbexpr1);

			// query 2
			assert (ast.getChild(0) != null);
			QBExpr qbexpr2 = new QBExpr(alias + "-subquery2");
			doPhase1QBExpr((ASTNode) ast.getChild(1), qbexpr2, id
					+ "-subquery2", alias + "-subquery2", ctx_1);
			qbexpr.setQBExpr2(qbexpr2);
		}
			break;
		}
	}

	/**
	 * 处理select语句，并把需要投影的部分记录起来
	 *
	 * @param ast
	 *            sparql语法树，取根节点为“SELECT_CLAUSE“
	 * @param querygraph
	 */
	private void processSelect(ASTNode ast, QueryGraph querygraph) {

		int count = ast.getChildCount();
		for (int i = 0; i < count; i++) {
			ASTNode child = (ASTNode) ast.getChild(i).getChild(0);
			querygraph.addProjection(child.getText());
			projections.add(child.getText());
		}
	}

	/**
	 * 处理where语句
	 *
	 * @param ast
	 *            sparql语法树，取根节点为“WHERE_CLAUSE“的下级“GROUP_GRAPH_PATTERN”
	 * @param querygraph
	 */
	private void processWhere(ASTNode ast, QueryGraph querygraph) {

		int count = ast.getChildCount();
		for (int i = 0; i < count; i++) {

			ASTNode child_pattern = (ASTNode) ast.getChild(i);

			// 判断是pattern（triples对）还是optional
			if (child_pattern.getToken().getType() == SparqlParser.TRIPLES_SAME_SUBJECT) {

				// 如果子节点数大于3，表示有多个pattern共用同一个subject
				int child_count = child_pattern.getChildCount();
				int pattern_count = (int) ((child_count - 1) / 2);

				// 首先获取SPO中 S 的信息
				String subject = null;
				boolean constSubject = false;

				// 第一个子节点为S，判断是否为url常量、普通字符串或者是变量
				int type = (((ASTNode) (child_pattern.getChild(0).getChild(0)))
						.getToken()).getType();
				if (type == SparqlParser.VAR1) {
					subject = child_pattern.getChild(0).getChild(0).getText();
					constSubject = false;
				} else if (type == SparqlParser.IRI_REF) {
					subject = child_pattern.getChild(0).getChild(0).getText();
					// 字符串转换
					subject = "<" + subject + ">";
					subject = String.valueOf(Driver.demo.searchTrie(subject));
					constSubject = true;
				}else if (type == SparqlParser.STRING_LITERAL2 || type == SparqlParser.INTEGER) {
					subject = child_pattern.getChild(0).getChild(0).getText();
					// 字符串转换
					subject = String.valueOf(Driver.demo.searchTrie(subject));
					constSubject = true;
				}

				// 接着分别获取SPO中 P 和 O 的信息
				for (int j = 0; j < pattern_count; j++) {

					String predicate = null;
					String object = null;
					boolean constPredicate = false;
					boolean constObject = false;

					for (int k = 1; k < 3; k++) {
						// SPO标识节点(k=1为P，k=2为O)
						ASTNode child_PO = (ASTNode) child_pattern.getChild(j
								* 2 + k);
						// SPO对应值节点
						ASTNode child_POName = (ASTNode) child_PO.getChild(0);

						if (child_POName.getToken().getType() == SparqlParser.VAR1) {
							switch (child_PO.getToken().getType()) {
							case SparqlParser.PREDICATE:
								predicate = child_POName.getText();
								constPredicate = false;
								break;
							case SparqlParser.OBJECT:
								object = child_POName.getText();
								constObject = false;
								break;
							default:
								break;
							}
						} else if (child_POName.getToken().getType() == SparqlParser.STRING_LITERAL2) {
							switch (child_PO.getToken().getType()) {
							case SparqlParser.PREDICATE:
								break;
							case SparqlParser.OBJECT:
								object = child_POName.getText();
								//object = object.replaceAll("\"", "");
								// 字符串转换
								object = String.valueOf(Driver.demo.searchTrie(object));
								constObject = true;
								break;
							default:
								break;
							}
						} else if (child_POName.getToken().getType() == SparqlParser.IRI_REF
								|| child_POName.getToken().getType() == SparqlParser.PATH) {
							switch (child_PO.getToken().getType()) {
							case SparqlParser.PREDICATE:
								predicate = child_POName.getChild(0)
										.getChild(0).getChild(0).getText();
								predicate = "<" + predicate + ">";
								// 字符串转换
								predicate = String.valueOf(Driver.demo.searchTrie(predicate));
								constPredicate = true;
								break;
							case SparqlParser.OBJECT:
								// 很奇怪对于object没有那么复杂的层次
								object = child_POName.getText();
								object = "<" + object + ">";
								// 字符串转换
								object = String.valueOf(Driver.demo.searchTrie(object));
								constObject = true;
								break;
							default:
								break;
							}
						}
					}

					QGNode node = new QGNode(subject, predicate, object,
							constSubject, constPredicate, constObject);
					querygraph.addNode(node);
					querygraph.getParseInfo().setSrcForAlias(node.alias,
						child_pattern);

					// unparseTranslator.addTableNameTranslation(tableTree,
					// db.getCurrentDatabase());
					// if (aliasIndex != 0) {
					// unparseTranslator.addIdentifierTranslation((ASTNode)
					// tabref
					// .getChild(aliasIndex));
					// }

				}
			} else if (child_pattern.getToken().getType() == SparqlParser.OPTIONAL) {

			}
		}
	}

	private void processFrom(ASTNode ast, QueryGraph querygraph) {

	}

	public void getMetaData(QueryGraph querygraph) throws SemanticException {
		getMetaData(querygraph, null);
	}

	public void getMetaData(QueryGraph querygraph, ReadEntity parentInput)
			throws SemanticException {

		try {

			LOG.info("Get metadata for source tables");

			// Go over the tables and populate the related structures.
			// We have to materialize the table alias list since we might
			// modify it in the middle for view rewrite.
			List<String> tabAliases = new ArrayList<String>(
					querygraph.getTabAliases());

			// Keep track of view alias to view name and read entity
			// For eg: for a query like 'select * from V3', where V3 -> V2, V2
			// -> V1, V1 -> T
			// keeps track of full view name and read entity corresponding to
			// alias V3, V3:V2, V3:V2:V1.
			// This is needed for tracking the dependencies for inputs, along
			// with their parents.
			Map<String, ObjectPair<String, ReadEntity>> aliasToViewInfo = new HashMap<String, ObjectPair<String, ReadEntity>>();
			for (String alias : tabAliases) {
				String tab_name = querygraph.getTabNameForAlias(alias);
				Table tab = null;
				try {
					tab = db.getTable(tab_name);
				} catch (InvalidTableException ite) {
					throw new SemanticException(
							ErrorMsg.INVALID_TABLE.getMsg(querygraph
									.getParseInfo().getSrcForAlias(alias)));
				}

				// Disallow INSERT INTO on bucketized tables
				if (querygraph.getParseInfo().isInsertIntoTable(
					tab.getDbName(), tab.getTableName())
						&& tab.getNumBuckets() > 0) {
					throw new SemanticException(
							ErrorMsg.INSERT_INTO_BUCKETIZED_TABLE
									.getMsg("Table: " + tab_name));
				}

				// We check offline of the table, as if people only select from
				// an
				// non-existing partition of an offline table, the partition
				// won't
				// be added to inputs and validate() won't have the information
				// to
				// check the table's offline status.
				// TODO: Modify the code to remove the checking here and
				// consolidate
				// it in validate()
				//
				if (tab.isOffline()) {
					throw new SemanticException(
							ErrorMsg.OFFLINE_TABLE_OR_PARTITION.getMsg("Table "
									+ getUnescapedName(querygraph
											.getParseInfo().getSrcForAlias(
												alias))));
				}

				// if (tab.isView()) {
				// if (querygraph.getParseInfo().isAnalyzeCommand()) {
				// throw new SemanticException(ErrorMsg.ANALYZE_VIEW.getMsg());
				// }
				// String fullViewName = tab.getDbName() + "." +
				// tab.getTableName();
				// // Prevent view cycles
				// if (viewsExpanded.contains(fullViewName)) {
				// throw new SemanticException("Recursive view " + fullViewName
				// +
				// " detected (cycle: " + StringUtils.join(viewsExpanded,
				// " -> ") +
				// " -> " + fullViewName + ").");
				// }
				// replaceViewReferenceWithDefinition(qb, tab, tab_name, alias);
				// // This is the last time we'll see the Table objects for
				// // views, so add it to the inputs now
				// ReadEntity viewInput = new ReadEntity(tab, parentInput);
				// viewInput = PlanUtils.addInput(inputs, viewInput);
				// aliasToViewInfo.put(alias, new ObjectPair<String,
				// ReadEntity>(fullViewName, viewInput));
				// viewAliasToInput.put(getAliasId(alias, qb), viewInput);
				// continue;
				// }

				if (!InputFormat.class.isAssignableFrom(tab
						.getInputFormatClass())) {
					throw new SemanticException(generateErrorMessage(querygraph
							.getParseInfo().getSrcForAlias(alias),
						ErrorMsg.INVALID_INPUT_FORMAT_TYPE.getMsg()));
				}

				querygraph.getMetaData().setSrcForAlias(alias, tab);

				if (querygraph.getParseInfo().isAnalyzeCommand()) {
					tableSpec ts = new tableSpec(db, conf,
							(ASTNode) ast.getChild(0));
					if (ts.specType == SpecType.DYNAMIC_PARTITION) { // dynamic
																		// partitions
						try {
							ts.partitions = db.getPartitionsByNames(
								ts.tableHandle, ts.partSpec);
						} catch (HiveException e) {
							throw new SemanticException(
									generateErrorMessage(
										querygraph.getParseInfo()
												.getSrcForAlias(alias),
										"Cannot get partitions for "
												+ ts.partSpec), e);
						}
					}
					querygraph.getParseInfo().addTableSpec(alias, ts);
				}
			}

			LOG.info("Get metadata for subqueries");
			// Go over the subqueries and getMetaData for these
			for (String alias : querygraph.getSubqAliases()) {
				boolean wasView = aliasToViewInfo.containsKey(alias);
				ReadEntity newParentInput = null;
				if (wasView) {
					// viewsExpanded.add(aliasToViewInfo.get(alias).getFirst());
					// newParentInput = aliasToViewInfo.get(alias).getSecond();
				}
				QBExpr qbexpr = querygraph.getSubqForAlias(alias);
				getMetaData(qbexpr, newParentInput);
				if (wasView) {
					// viewsExpanded.remove(viewsExpanded.size() - 1);
				}
			}

			LOG.info("Get metadata for destination tables");
			// Go over all the destination structures and populate the related
			// metadata
			QBParseInfo qbp = querygraph.getParseInfo();

			for (String name : qbp.getClauseNamesForDest()) {
				// ASTNode ast = qbp.getDestForClause(name);
				// switch (ast.getToken().getType()) {
				// case HiveParser.TOK_TAB: {
				// tableSpec ts = new tableSpec(db, conf, ast);
				// if (ts.tableHandle.isView()) {
				// throw new SemanticException(
				// ErrorMsg.DML_AGAINST_VIEW.getMsg());
				// }
				//
				// Class<?> outputFormatClass = ts.tableHandle
				// .getOutputFormatClass();
				// if (!HiveOutputFormat.class
				// .isAssignableFrom(outputFormatClass)) {
				// throw new SemanticException(
				// ErrorMsg.INVALID_OUTPUT_FORMAT_TYPE.getMsg(
				// ast,
				// "The class is "
				// + outputFormatClass.toString()));
				// }
				//
				// // tableSpec ts is got from the query (user specified),
				// // which means the user didn't specify partitions in their
				// // query,
				// // but whether the table itself is partitioned is not know.
				// if (ts.specType != SpecType.STATIC_PARTITION) {
				// // This is a table or dynamic partition
				// qb.getMetaData().setDestForAlias(name, ts.tableHandle);
				// // has dynamic as well as static partitions
				// if (ts.partSpec != null && ts.partSpec.size() > 0) {
				// qb.getMetaData().setPartSpecForAlias(name,
				// ts.partSpec);
				// }
				// } else {
				// // This is a partition
				// qb.getMetaData().setDestForAlias(name, ts.partHandle);
				// }
				// if (HiveConf.getBoolVar(conf,
				// HiveConf.ConfVars.HIVESTATSAUTOGATHER)) {
				// // Set that variable to automatically collect stats
				// // during the MapReduce job
				// qb.getParseInfo().setIsInsertToTable(true);
				// // Add the table spec for the destination table.
				// qb.getParseInfo().addTableSpec(
				// ts.tableName.toLowerCase(), ts);
				// }
				// break;
				// }
				//
				// case HiveParser.TOK_LOCAL_DIR:
				// case HiveParser.TOK_DIR: {
				// This is a dfs file
				// String fname = stripQuotes(ast.getChild(0).getText());
				String fname = "TOK_TMP_FILE";
				if (!querygraph.getParseInfo().getIsSubQ()) {
					// if ((!querygraph.getParseInfo().getIsSubQ())
					// && (((ASTNode) ast.getChild(0)).getToken().getType() ==
					// HiveParser.TOK_TMP_FILE)) {
					//
					// if (querygraph.isCTAS()) {
					// querygraph.setIsQuery(false);
					// ctx.setResDir(null);
					// ctx.setResFile(null);
					//
					// // allocate a temporary output dir on the location
					// // of the table
					// String tableName = getUnescapedName((ASTNode) ast
					// .getChild(0));
					// Table newTable = db.newTable(tableName);
					// Path location;
					// try {
					// Warehouse wh = new Warehouse(conf);
					// location = wh.getDatabasePath(db
					// .getDatabase(newTable.getDbName()));
					// } catch (MetaException e) {
					// throw new SemanticException(e);
					// }
					// try {
					// fname = ctx.getExternalTmpFileURI(FileUtils
					// .makeQualified(location, conf).toUri());
					// } catch (Exception e) {
					// throw new SemanticException(generateErrorMessage(
					// ast, "Error creating temporary folder on: "
					// + location.toString()), e);
					// }
					// if (HiveConf.getBoolVar(conf,
					// HiveConf.ConfVars.HIVESTATSAUTOGATHER)) {
					// tableSpec ts = new tableSpec(db, conf, this.ast);
					// // Set that variable to automatically collect
					// // stats during the MapReduce job
					// qb.getParseInfo().setIsInsertToTable(true);
					// // Add the table spec for the destination table.
					// qb.getParseInfo().addTableSpec(
					// ts.tableName.toLowerCase(), ts);
					// }
					// } else {

					querygraph.setIsQuery(true);
					fname = ctx.getMRTmpFileURI();
					ctx.setResDir(new Path(fname));
				}
				// }
				querygraph.getMetaData().setDestForAlias(name, fname, true);
				// break;
				// }
				// default:
				// throw new SemanticException(generateErrorMessage(ast,
				// "Unknown Token Type " + ast.getToken().getType()));
				// }
			}
		} catch (HiveException e) {
			// Has to use full name to make sure it does not conflict with
			// org.apache.commons.lang.StringUtils
			LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
			throw new SemanticException(e.getMessage(), e);
		}

	}

	private void getMetaData(QBExpr qbexpr, ReadEntity parentInput)
			throws SemanticException {
		if (qbexpr.getOpcode() == QBExpr.Opcode.NULLOP) {
			getMetaData((QueryGraph) qbexpr.getQB(), parentInput);
		} else {
			getMetaData(qbexpr.getQBExpr1(), parentInput);
			getMetaData(qbexpr.getQBExpr2(), parentInput);
		}
	}

	@SuppressWarnings({ "nls", "rawtypes" })
	public Operator genPlan(QueryGraph querygraph) throws SemanticException {

		// First generate all the opInfos for the elements in the from clause
		Map<String, Operator> aliasToOpInfo = new HashMap<String, Operator>();

		// Recurse over the subqueries to fill the subquery part of the plan
		for (String alias : querygraph.getSubqAliases()) {
			QBExpr qbexpr = querygraph.getSubqForAlias(alias);
			aliasToOpInfo.put(alias, genPlan(qbexpr));
			qbexpr.setAlias(alias);
		}

		// Recurse over all the source tables
		for (String alias : querygraph.getTabAliases()) {
			Operator op = genTablePlan(alias, querygraph);
			aliasToOpInfo.put(alias, op);
		}

		// For all the source tables that have a lateral view, attach the
		// appropriate operators to the TS
		// genLateralViewPlans(aliasToOpInfo, qb);

		Operator srcOpInfo = null;

		// process join 在此对应于第一个 Group_graph_pattern 节点
		if (querygraph.getParseInfo().getJoinExpr() != null) {
			ASTNode joinExpr = querygraph.getParseInfo().getJoinExpr();

			// if (joinExpr.getToken().getType() == HiveParser.TOK_UNIQUEJOIN) {
			// QBJoinTree joinTree = genUniqueJoinTree(qb, joinExpr,
			// aliasToOpInfo);
			// qb.setQbJoinTree(joinTree);
			// } else {
			SparqlJoinTree joinTree = genJoinTree(querygraph, joinExpr,
				aliasToOpInfo);
			querygraph.setQbJoinTree(joinTree);
			mergeJoinTree(querygraph);
			// }

			// if any filters are present in the join tree, push them on top of
			// the table
			pushJoinFilters(querygraph,
				(SparqlJoinTree) querygraph.getQbJoinTree(), aliasToOpInfo);
			srcOpInfo = genJoinPlan(querygraph, aliasToOpInfo);
		} else {
			// Now if there are more than 1 sources then we have a join case
			// later we can extend this to the union all case as well
			srcOpInfo = aliasToOpInfo.values().iterator().next();
		}

		Operator bodyOpInfo = genBodyPlan(querygraph, srcOpInfo);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Created Plan for Query Block " + querygraph.getId());
		}

		this.querygraph = querygraph;
		return bodyOpInfo;
	}

	@SuppressWarnings("rawtypes")
	private Operator genPlan(QBExpr qbexpr) throws SemanticException {
		if (qbexpr.getOpcode() == QBExpr.Opcode.NULLOP) {
			return genPlan((QueryGraph) qbexpr.getQB());
		}
		if (qbexpr.getOpcode() == QBExpr.Opcode.UNION) {
			Operator qbexpr1Ops = genPlan(qbexpr.getQBExpr1());
			Operator qbexpr2Ops = genPlan(qbexpr.getQBExpr2());

			return genUnionPlan(qbexpr.getAlias(), qbexpr.getQBExpr1()
					.getAlias(), qbexpr1Ops, qbexpr.getQBExpr2().getAlias(),
				qbexpr2Ops);
		}
		return null;
	}

	@SuppressWarnings({ "nls", "rawtypes", "unchecked" })
	private Operator genUnionPlan(String unionalias, String leftalias,
			Operator leftOp, String rightalias, Operator rightOp)
			throws SemanticException {

		// Currently, the unions are not merged - each union has only 2 parents.
		// So,
		// a n-way union will lead to (n-1) union operators.
		// This can be easily merged into 1 union
		RowResolver leftRR = opParseCtx.get(leftOp).getRowResolver();
		RowResolver rightRR = opParseCtx.get(rightOp).getRowResolver();
		HashMap<String, ColumnInfo> leftmap = leftRR.getFieldMap(leftalias);
		HashMap<String, ColumnInfo> rightmap = rightRR.getFieldMap(rightalias);
		// make sure the schemas of both sides are the same
		ASTNode tabref = querygraph.getAliases().isEmpty() ? null : querygraph
				.getParseInfo().getSrcForAlias(querygraph.getAliases().get(0));
		if (leftmap.size() != rightmap.size()) {
			throw new SemanticException(
					"Schema of both sides of union should match.");
		}
		for (Map.Entry<String, ColumnInfo> lEntry : leftmap.entrySet()) {
			String field = lEntry.getKey();
			ColumnInfo lInfo = lEntry.getValue();
			ColumnInfo rInfo = rightmap.get(field);
			if (rInfo == null) {
				throw new SemanticException(generateErrorMessage(tabref,
					"Schema of both sides of union should match. " + rightalias
							+ " does not have the field " + field));
			}
			if (lInfo == null) {
				throw new SemanticException(generateErrorMessage(tabref,
					"Schema of both sides of union should match. " + leftalias
							+ " does not have the field " + field));
			}
			if (!lInfo.getInternalName().equals(rInfo.getInternalName())) {
				throw new SemanticException(
						generateErrorMessage(
							tabref,
							"Schema of both sides of union should match: field "
									+ field
									+ ":"
									+ " appears on the left side of the UNION at column position: "
									+ getPositionFromInternalName(lInfo
											.getInternalName())
									+ ", and on the right side of the UNION at column position: "
									+ getPositionFromInternalName(rInfo
											.getInternalName())
									+ ". Column positions should match for a UNION"));
			}
			// try widening coversion, otherwise fail union
			TypeInfo commonTypeInfo = FunctionRegistry
					.getCommonClassForUnionAll(lInfo.getType(), rInfo.getType());
			if (commonTypeInfo == null) {
				throw new SemanticException(generateErrorMessage(tabref,
					"Schema of both sides of union should match: Column "
							+ field + " is of type "
							+ lInfo.getType().getTypeName()
							+ " on first table and type "
							+ rInfo.getType().getTypeName()
							+ " on second table"));
			}
		}

		// construct the forward operator
		RowResolver unionoutRR = new RowResolver();
		for (Map.Entry<String, ColumnInfo> lEntry : leftmap.entrySet()) {
			String field = lEntry.getKey();
			ColumnInfo lInfo = lEntry.getValue();
			ColumnInfo rInfo = rightmap.get(field);
			ColumnInfo unionColInfo = new ColumnInfo(lInfo);
			unionColInfo.setType(FunctionRegistry.getCommonClassForUnionAll(
				lInfo.getType(), rInfo.getType()));
			unionoutRR.put(unionalias, field, unionColInfo);
		}

		if (!(leftOp instanceof UnionOperator)) {
			leftOp = genInputSelectForUnion(leftOp, leftmap, leftalias,
				unionoutRR, unionalias);
		}

		if (!(rightOp instanceof UnionOperator)) {
			rightOp = genInputSelectForUnion(rightOp, rightmap, rightalias,
				unionoutRR, unionalias);
		}

		// If one of the children is a union, merge with it
		// else create a new one
		if ((leftOp instanceof UnionOperator)
				|| (rightOp instanceof UnionOperator)) {
			if (leftOp instanceof UnionOperator) {
				// make left a child of right
				List<Operator<? extends OperatorDesc>> child = new ArrayList<Operator<? extends OperatorDesc>>();
				child.add(leftOp);
				rightOp.setChildOperators(child);

				List<Operator<? extends OperatorDesc>> parent = leftOp
						.getParentOperators();
				parent.add(rightOp);

				UnionDesc uDesc = ((UnionOperator) leftOp).getConf();
				uDesc.setNumInputs(uDesc.getNumInputs() + 1);
				return putOpInsertMap(leftOp, unionoutRR);
			} else {
				// make right a child of left
				List<Operator<? extends OperatorDesc>> child = new ArrayList<Operator<? extends OperatorDesc>>();
				child.add(rightOp);
				leftOp.setChildOperators(child);

				List<Operator<? extends OperatorDesc>> parent = rightOp
						.getParentOperators();
				parent.add(leftOp);
				UnionDesc uDesc = ((UnionOperator) rightOp).getConf();
				uDesc.setNumInputs(uDesc.getNumInputs() + 1);

				return putOpInsertMap(rightOp, unionoutRR);
			}
		}

		// Create a new union operator
		Operator<? extends OperatorDesc> unionforward = OperatorFactory
				.getAndMakeChild(new UnionDesc(),
					new RowSchema(unionoutRR.getColumnInfos()));

		// set union operator as child of each of leftOp and rightOp
		List<Operator<? extends OperatorDesc>> child = new ArrayList<Operator<? extends OperatorDesc>>();
		child.add(unionforward);
		rightOp.setChildOperators(child);

		child = new ArrayList<Operator<? extends OperatorDesc>>();
		child.add(unionforward);
		leftOp.setChildOperators(child);

		List<Operator<? extends OperatorDesc>> parent = new ArrayList<Operator<? extends OperatorDesc>>();
		parent.add(leftOp);
		parent.add(rightOp);
		unionforward.setParentOperators(parent);

		// create operator info list to return
		return putOpInsertMap(unionforward, unionoutRR);
	}

	/**
	 * Generates a select operator which can go between the original input
	 * operator and the union operator. This select casts columns to match the
	 * type of the associated column in the union, other columns pass through
	 * unchanged. The new operator's only parent is the original input operator
	 * to the union, and it's only child is the union. If the input does not
	 * need to be cast, the original operator is returned, and no new select
	 * operator is added.
	 *
	 * @param origInputOp
	 *            The original input operator to the union.
	 * @param origInputFieldMap
	 *            A map from field name to ColumnInfo for the original input
	 *            operator.
	 * @param origInputAlias
	 *            The alias associated with the original input operator.
	 * @param unionoutRR
	 *            The union's output row resolver.
	 * @param unionalias
	 *            The alias of the union.
	 * @return
	 * @throws UDFArgumentException
	 */
	private Operator<? extends OperatorDesc> genInputSelectForUnion(
			Operator<? extends OperatorDesc> origInputOp,
			Map<String, ColumnInfo> origInputFieldMap, String origInputAlias,
			RowResolver unionoutRR, String unionalias)
			throws UDFArgumentException {

		List<ExprNodeDesc> columns = new ArrayList<ExprNodeDesc>();
		boolean needsCast = false;
		for (Map.Entry<String, ColumnInfo> unionEntry : unionoutRR.getFieldMap(
			unionalias).entrySet()) {
			String field = unionEntry.getKey();
			ColumnInfo lInfo = origInputFieldMap.get(field);
			ExprNodeDesc column = new ExprNodeColumnDesc(lInfo.getType(),
					lInfo.getInternalName(), lInfo.getTabAlias(),
					lInfo.getIsVirtualCol(), lInfo.isSkewedCol());
			if (!lInfo.getType().equals(unionEntry.getValue().getType())) {
				needsCast = true;
				column = TypeCheckProcFactory.DefaultExprProcessor
						.getFuncExprNodeDesc(unionEntry.getValue().getType()
								.getTypeName(), column);
			}
			columns.add(column);
		}

		// If none of the columns need to be cast there's no need for an
		// additional select operator
		if (!needsCast) {
			return origInputOp;
		}

		RowResolver rowResolver = new RowResolver();
		List<String> colName = new ArrayList<String>();
		for (int i = 0; i < columns.size(); i++) {
			String name = getColumnInternalName(i);
			rowResolver.put(origInputAlias, name, new ColumnInfo(name, columns
					.get(i).getTypeInfo(), "", false));
			colName.add(name);
		}

		Operator<SelectDesc> newInputOp = OperatorFactory.getAndMakeChild(
			new SelectDesc(columns, colName),
			new RowSchema(rowResolver.getColumnInfos()), origInputOp);
		return putOpInsertMap(newInputOp, rowResolver);
	}

	/**
	 * Extract the filters from the join condition and push them on top of the
	 * source operators. This procedure traverses the query tree recursively,
	 */
	@SuppressWarnings("rawtypes")
	private void pushJoinFilters(QueryGraph querygraph,
			SparqlJoinTree joinTree, Map<String, Operator> map)
			throws SemanticException {
		if (joinTree.getJoinSrc() != null) {
			pushJoinFilters(querygraph, (SparqlJoinTree) joinTree.getJoinSrc(),
				map);
		}
		// ArrayList<ArrayList<ASTNode>> filters =
		// joinTree.getFiltersForPushing();
		// int pos = 0;
		for (String src : joinTree.getBaseSrc()) {
			if (src != null) {
				Operator srcOp = map.get(src);
				// ArrayList<ASTNode> filter = filters.get(pos);
				// for (ASTNode cond : filter) {
				// srcOp = genFilterPlan(qb, cond, srcOp);
				// }
				map.put(src, srcOp);
			}
			// pos++;
		}
	}

	private String getAliasId(String alias, QueryGraph querygraph) {
		return (querygraph.getId() == null ? alias : querygraph.getId() + ":"
				+ alias);
	}

	@SuppressWarnings({ "nls", "rawtypes" })
	private Operator genTablePlan(String alias, QueryGraph querygraph)
			throws SemanticException {

		String alias_id = getAliasId(alias, querygraph);
		Table tab = querygraph.getMetaData().getSrcForAlias(alias);
		RowResolver rwsch;

		// is the table already present
		Operator<? extends OperatorDesc> top = topOps.get(alias_id);
		Operator<? extends OperatorDesc> dummySel = topSelOps.get(alias_id);
		if (dummySel != null) {
			top = dummySel;
		}

		if (top == null) {
			rwsch = new RowResolver();
			try {
				StructObjectInspector rowObjectInspector = (StructObjectInspector) tab
						.getDeserializer().getObjectInspector();
				List<? extends StructField> fields = rowObjectInspector
						.getAllStructFieldRefs();
				for (int i = 0; i < fields.size(); i++) {
					/**
					 * if the column is a skewed column, use ColumnInfo
					 * accordingly
					 */
					ColumnInfo colInfo = new ColumnInfo(fields.get(i)
							.getFieldName(),
							TypeInfoUtils.getTypeInfoFromObjectInspector(fields
									.get(i).getFieldObjectInspector()), alias,
							false);
					colInfo.setSkewedCol((isSkewedCol(alias, querygraph, fields
							.get(i).getFieldName())) ? true : false);
					rwsch.put(alias, fields.get(i).getFieldName(), colInfo);
				}
			} catch (SerDeException e) {
				throw new RuntimeException(e);
			}

	      // Finally add the partitioning columns
	      for (FieldSchema part_col : tab.getPartCols()) {
	        LOG.trace("Adding partition col: " + part_col);
	        // TODO: use the right type by calling part_col.getType() instead of
	        // String.class
	        rwsch.put(alias, part_col.getName(), new ColumnInfo(part_col.getName(),
	            TypeInfoFactory.stringTypeInfo, alias, true));
	      }

			// put all virutal columns in RowResolver.
			Iterator<VirtualColumn> vcs = VirtualColumn.getRegistry(conf)
					.iterator();
			// use a list for easy cumtomize
			List<VirtualColumn> vcList = new ArrayList<VirtualColumn>();
			while (vcs.hasNext()) {
				VirtualColumn vc = vcs.next();
				rwsch.put(alias, vc.getName(),
					new ColumnInfo(vc.getName(), vc.getTypeInfo(), alias, true,
							vc.getIsHidden()));
				vcList.add(vc);
			}

			// Create the root of the operator tree
			TableScanDesc tsDesc = new TableScanDesc(alias, vcList);
			tsDesc.setGatherStats(false);

			top = putOpInsertMap(
				OperatorFactory.get(tsDesc,
					new RowSchema(rwsch.getColumnInfos())), rwsch);

			// Add this to the list of top operators - we always start from a
			// table scan
			topOps.put(alias_id, top);

			// Add a mapping from the table scan operator to Table
			topToTable.put((TableScanOperator) top, tab);
		} else {
			rwsch = opParseCtx.get(top).getRowResolver();
			top.setChildOperators(null);
		}

		Operator output = putOpInsertMap(top, rwsch);

		return output;
	}

	private boolean isSkewedCol(String alias, QueryGraph querygraph,
			String colName) {
		boolean isSkewedCol = false;
		List<String> skewedCols = querygraph.getSkewedColumnNames(alias);
		for (String skewedCol : skewedCols) {
			if (skewedCol.equalsIgnoreCase(colName)) {
				isSkewedCol = true;
			}
		}
		return isSkewedCol;
	}

	@SuppressWarnings("nls")
	public <T extends OperatorDesc> Operator<T> putOpInsertMap(Operator<T> op,
			RowResolver rr) {
		OpParseContext ctx = new OpParseContext(rr);
		opParseCtx.put(op, ctx);
		op.augmentPlan();
		return op;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private SparqlJoinTree genJoinTree(QueryGraph querygraph,
			ASTNode joinParseTree, Map<String, Operator> aliasToOpInfo)
			throws SemanticException {

		SparqlJoinTree joinTree = null;

		HashMap<Integer, QGEdge> edges = querygraph.getEdges();

		for (QGEdge edge : edges.values()) {

			// 一对pattern可能有2个或以上的连接，因此需要
			// 多建立几个节点。
			int joinSize = edge.joins.size();

			for (int i = 0; i < joinSize; i++) {
				JoinCond[] condn = new JoinCond[1];
				condn[0] = new JoinCond(0, 1, JoinType.INNER);

				if (joinTree == null) {
					joinTree = new SparqlJoinTree();

					// 设置连接方式
					joinTree.setNoOuterJoin(true);
					joinTree.setJoinCond(condn);

					// String tableName = QueryGraph.getTablename();
					String alias = edge.first.alias;

					// 保存连接的左边信息
					joinTree.setJoininfo(edge);
					joinTree.setLeftAlias(alias);
					String[] leftAliases = new String[1];
					leftAliases[0] = alias;
					joinTree.setLeftAliases(leftAliases);
					String[] children = new String[2];
					children[0] = alias;
					joinTree.setBaseSrc(children);
					joinTree.setId(querygraph.getId());
					joinTree.getAliasToOpInfo().put(
						getModifiedAlias(querygraph, alias),
						aliasToOpInfo.get(alias));
				} else {

//					//假如正要连接的表在前面已经连接过，那么就退出
//					boolean flag = false;
//					for (String str1 : joinTree.getLeftAliases()) {
//						for (String str2 : joinTree.getRightAliases()) {
//							if (str1.compareToIgnoreCase(edge.first.alias) == 0
//									&& str2.compareToIgnoreCase(edge.second.alias) == 0) {
//								flag = true;
//							}
//							if (str2.compareToIgnoreCase(edge.first.alias) == 0
//									&& str1.compareToIgnoreCase(edge.second.alias) == 0) {
//								flag = true;
//							}
//						}
//					}
//					for (String str1 : joinTree.getLeftAliases()) {
//						if (str1.equalsIgnoreCase(edge.first.alias)){
//							for (String str2 : joinTree.getLeftAliases()) {
//								if (str2.equalsIgnoreCase(edge.second.alias)){
//									flag = true;
//								}
//							}
//						}
//					}
//
//					if (flag)
//						break;

					SparqlJoinTree leftTree = joinTree;
					joinTree = new SparqlJoinTree();
					joinTree.setJoininfo(edge);

					// 设置连接方式
					joinTree.setNoOuterJoin(true);
					joinTree.setJoinCond(condn);

					// 总结上一个连接的左边信息
					joinTree.setJoinSrc(leftTree);
					String[] leftChildAliases = leftTree.getLeftAliases();
					String leftAliases[] = new String[leftChildAliases.length + 1];
					for (int j = 0; j < leftChildAliases.length; j++) {
						leftAliases[j] = leftChildAliases[j];
					}
					leftAliases[leftChildAliases.length] = leftTree
							.getRightAliases()[0];
					joinTree.setLeftAliases(leftAliases);
				}

				// String tableName = QueryGraph.getTablename();
				String alias = edge.second.alias;
				String[] rightAliases = new String[1];
				rightAliases[0] = alias;
				joinTree.setRightAliases(rightAliases);
				String[] children = joinTree.getBaseSrc();
				if (children == null) {
					children = new String[2];
				}
				children[1] = alias;
				joinTree.setBaseSrc(children);
				joinTree.setId(querygraph.getId());
				joinTree.getAliasToOpInfo().put(
					getModifiedAlias(querygraph, alias),
					aliasToOpInfo.get(alias));
				// remember rhs table for semijoin
				if (joinTree.getNoSemiJoin() == false) {
					joinTree.addRHSSemijoin(alias);
				}

				ArrayList<ArrayList<ASTNode>> expressions = new ArrayList<ArrayList<ASTNode>>();
				expressions.add(new ArrayList<ASTNode>());
				expressions.add(new ArrayList<ASTNode>());
				joinTree.setExpressions(expressions);

				ArrayList<Boolean> nullsafes = new ArrayList<Boolean>();
				joinTree.setNullSafes(nullsafes);

				ArrayList<ArrayList<ASTNode>> filters = new ArrayList<ArrayList<ASTNode>>();
				filters.add(new ArrayList<ASTNode>());
				filters.add(new ArrayList<ASTNode>());
				joinTree.setFilters(filters);
				joinTree.setFilterMap(new int[2][]);

				ArrayList<ArrayList<ASTNode>> filtersForPushing = new ArrayList<ArrayList<ASTNode>>();
				filtersForPushing.add(new ArrayList<ASTNode>());
				filtersForPushing.add(new ArrayList<ASTNode>());
				joinTree.setFiltersForPushing(filtersForPushing);

				ArrayList<String> leftSrc = new ArrayList<String>();
				// 记录连接条件
				saveJoinCondn(joinTree, edge, edge.joins.get(i));
				parseJoinCondition(joinTree, edge, leftSrc);
				if (leftSrc.size() == 1) {
					joinTree.setLeftAlias(leftSrc.get(0));
				}

				// check the hints to see if the user has specified a map-side
				// join.
				// This
				// will be removed later on, once the cost-based
				// infrastructure is in place
				// if (querygraph.getParseInfo().getHints() != null) {
				// List<String> mapSideTables =
				// getMapSideJoinTables(querygraph);
				// List<String> mapAliases = joinTree.getMapAliases();
				//
				// for (String mapTbl : mapSideTables) {
				// boolean mapTable = false;
				// for (String leftAlias : joinTree.getLeftAliases()) {
				// if (mapTbl.equalsIgnoreCase(leftAlias)) {
				// mapTable = true;
				// }
				// }
				// for (String rightAlias : joinTree.getRightAliases()) {
				// if (mapTbl.equalsIgnoreCase(rightAlias)) {
				// mapTable = true;
				// }
				// }
				//
				// if (mapTable) {
				// if (mapAliases == null) {
				// mapAliases = new ArrayList<String>();
				// }
				// mapAliases.add(mapTbl);
				// joinTree.setMapSideJoin(true);
				// }
				// }
				//
				// joinTree.setMapAliases(mapAliases);
				//
				// parseStreamTables(joinTree, qb);
				// }
			}
		}
		return joinTree;
	}

	private void mergeJoinTree(QueryGraph querygraph) {
		SparqlJoinTree root = (SparqlJoinTree) querygraph.getQbJoinTree();
		SparqlJoinTree parent = null;
		while (root != null) {
			boolean merged = mergeJoinNodes(querygraph, parent, root,
				(SparqlJoinTree) root.getJoinSrc());

			if (parent == null) {
				if (merged) {
					root = (SparqlJoinTree) querygraph.getQbJoinTree();
				} else {
					parent = root;
					root = (SparqlJoinTree) root.getJoinSrc();
				}
			} else {
				if (merged) {
					root = (SparqlJoinTree) root.getJoinSrc();
				} else {
					parent = (SparqlJoinTree) parent.getJoinSrc();
					root = (SparqlJoinTree) parent.getJoinSrc();
				}
			}
		}
	}

	private boolean mergeJoinNodes(QueryGraph querygraph,
			SparqlJoinTree parent, SparqlJoinTree node, SparqlJoinTree target) {
		if (target == null) {
			return false;
		}
		if (!node.getNoOuterJoin() || !target.getNoOuterJoin()) {
			// todo 8 way could be not enough number
			if (node.getRightAliases().length + node.getRightAliases().length
					+ 1 >= 8) {
				LOG.info(ErrorMsg.JOINNODE_OUTERJOIN_MORETHAN_8);
				return false;
			}
		}
		int res = findMergePos(node, target);
		if (res != -1) {
			mergeJoins(querygraph, parent, node, target, res);
			return true;
		}

		return mergeJoinNodes(querygraph, parent, node,
			(SparqlJoinTree) target.getJoinSrc());
	}

	private void mergeJoins(QueryGraph querygraph, SparqlJoinTree parent,
			SparqlJoinTree node, SparqlJoinTree target, int pos) {
		String[] nodeRightAliases = node.getRightAliases();
		String[] trgtRightAliases = target.getRightAliases();
		String[] rightAliases = new String[nodeRightAliases.length
				+ trgtRightAliases.length];

		for (int i = 0; i < trgtRightAliases.length; i++) {
			rightAliases[i] = trgtRightAliases[i];
		}
		for (int i = 0; i < nodeRightAliases.length; i++) {
			rightAliases[i + trgtRightAliases.length] = nodeRightAliases[i];
		}
		target.setRightAliases(rightAliases);
		target.getAliasToOpInfo().putAll(node.getAliasToOpInfo());

		String[] nodeBaseSrc = node.getBaseSrc();
		String[] trgtBaseSrc = target.getBaseSrc();
		String[] baseSrc = new String[nodeBaseSrc.length + trgtBaseSrc.length
				- 1];

		for (int i = 0; i < trgtBaseSrc.length; i++) {
			baseSrc[i] = trgtBaseSrc[i];
		}
		for (int i = 1; i < nodeBaseSrc.length; i++) {
			baseSrc[i + trgtBaseSrc.length - 1] = nodeBaseSrc[i];
		}
		target.setBaseSrc(baseSrc);

		//ArrayList<ArrayList<ASTNode>> expr = target.getExpressions();
		for (int i = 0; i < nodeRightAliases.length; i++) {
			//expr.add(node.getExpressions().get(i + 1));
			target.addJoinKeys(node.getJoinKeys().get(i + 1));
		}

		ArrayList<Boolean> nns = node.getNullSafes();
		ArrayList<Boolean> tns = target.getNullSafes();
		for (int i = 0; i < tns.size(); i++) {
			tns.set(i, tns.get(i) & nns.get(i)); // any of condition contains
													// non-NS, non-NS
		}

		ArrayList<ArrayList<ASTNode>> filters = target.getFilters();
		for (int i = 0; i < nodeRightAliases.length; i++) {
			filters.add(node.getFilters().get(i + 1));
		}

		if (node.getFilters().get(0).size() != 0) {
			ArrayList<ASTNode> filterPos = filters.get(pos);
			filterPos.addAll(node.getFilters().get(0));
		}

		int[][] nmap = node.getFilterMap();
		int[][] tmap = target.getFilterMap();
		int[][] newmap = new int[tmap.length + nmap.length - 1][];

		for (int[] mapping : nmap) {
			if (mapping != null) {
				for (int i = 0; i < mapping.length; i += 2) {
					if (pos > 0 || mapping[i] > 0) {
						mapping[i] += trgtRightAliases.length;
					}
				}
			}
		}
		if (nmap[0] != null) {
			if (tmap[pos] == null) {
				tmap[pos] = nmap[0];
			} else {
				int[] appended = new int[tmap[pos].length + nmap[0].length];
				System.arraycopy(tmap[pos], 0, appended, 0, tmap[pos].length);
				System.arraycopy(nmap[0], 0, appended, tmap[pos].length,
					nmap[0].length);
				tmap[pos] = appended;
			}
		}
		System.arraycopy(tmap, 0, newmap, 0, tmap.length);
		System.arraycopy(nmap, 1, newmap, tmap.length, nmap.length - 1);
		target.setFilterMap(newmap);

		ArrayList<ArrayList<ASTNode>> filter = target.getFiltersForPushing();
		for (int i = 0; i < nodeRightAliases.length; i++) {
			filter.add(node.getFiltersForPushing().get(i + 1));
		}

		if (node.getFiltersForPushing().get(0).size() != 0) {
			ArrayList<ASTNode> filterPos = filter.get(pos);
			filterPos.addAll(node.getFiltersForPushing().get(0));
		}

		if (querygraph.getQbJoinTree() == node) {
			querygraph.setQbJoinTree(node.getJoinSrc());
		} else {
			parent.setJoinSrc(node.getJoinSrc());
		}

		if (node.getNoOuterJoin() && target.getNoOuterJoin()) {
			target.setNoOuterJoin(true);
		} else {
			target.setNoOuterJoin(false);
		}

		if (node.getNoSemiJoin() && target.getNoSemiJoin()) {
			target.setNoSemiJoin(true);
		} else {
			target.setNoSemiJoin(false);
		}

		target.mergeRHSSemijoin(node);

		JoinCond[] nodeCondns = node.getJoinCond();
		int nodeCondnsSize = nodeCondns.length;
		JoinCond[] targetCondns = target.getJoinCond();
		int targetCondnsSize = targetCondns.length;
		JoinCond[] newCondns = new JoinCond[nodeCondnsSize + targetCondnsSize];
		for (int i = 0; i < targetCondnsSize; i++) {
			newCondns[i] = targetCondns[i];
		}

		for (int i = 0; i < nodeCondnsSize; i++) {
			JoinCond nodeCondn = nodeCondns[i];
			if (nodeCondn.getLeft() == 0) {
				nodeCondn.setLeft(pos);
			} else {
				nodeCondn.setLeft(nodeCondn.getLeft() + targetCondnsSize);
			}
			nodeCondn.setRight(nodeCondn.getRight() + targetCondnsSize);
			newCondns[targetCondnsSize + i] = nodeCondn;
		}

		target.setJoinCond(newCondns);
		if (target.isMapSideJoin()) {
			assert node.isMapSideJoin();
			List<String> mapAliases = target.getMapAliases();
			for (String mapTbl : node.getMapAliases()) {
				if (!mapAliases.contains(mapTbl)) {
					mapAliases.add(mapTbl);
				}
			}
			target.setMapAliases(mapAliases);
		}
	}

	private int findMergePos(SparqlJoinTree node, SparqlJoinTree target) {
		int res = -1;
		String leftAlias = node.getLeftAlias();
		if (leftAlias == null) {
			return -1;
		}

		// ArrayList<ASTNode> nodeCondn = node.getExpressions().get(0);
		// ArrayList<ASTNode> targetCondn = null;
		String[] nodeCondn = node.getJoinKeys().get(0);
		String[] targetCondn = null;

		if (leftAlias.equals(target.getLeftAlias())) {
			targetCondn = target.getJoinKeys().get(0);
			res = 0;
		} else {
			for (int i = 0; i < target.getRightAliases().length; i++) {
				if (leftAlias.equals(target.getRightAliases()[i])) {
					targetCondn = target.getJoinKeys().get(i + 1);
					res = i + 1;
					break;
				}
			}
		}

		 if ((targetCondn == null) || (nodeCondn.length !=
		 targetCondn.length)) {
		 return -1;
		 }

		// for (int i = 0; i < nodeCondn.size(); i++) {
		if (!nodeCondn[0].equals(targetCondn[0])
				|| !nodeCondn[1].equals(targetCondn[1])) {
			return -1;
		}
		// }

		return res;
	}

	private void saveJoinCondn(SparqlJoinTree joinTree, QGEdge edge,
			String[] joinKey) {
		// TODO Auto-generated method stub
		String[] left = new String[2];
		String[] right = new String[2];
		left[0] = joinKey[0];
		right[0] = joinKey[2];

		left[1] = joinKey[1];
		right[1] = joinKey[3];

		joinTree.setLeftAlias(left[0]);
		joinTree.addJoinKeys(left);
		joinTree.addJoinKeys(right);

	}

	private String getModifiedAlias(QueryGraph querygraph, String alias) {
		return QueryGraph.getAppendedAliasFromId(querygraph.getId(), alias);
	}

	private void parseJoinCondition(SparqlJoinTree joinTree, QGEdge edge,
			List<String> leftSrc) throws SemanticException {
		if (edge == null) {
			return;
		}
		JoinCond cond = joinTree.getJoinCond()[0];

		JoinType type = cond.getJoinType();
		parseJoinCondition(joinTree, edge, leftSrc, type);

		List<ArrayList<ASTNode>> filters = joinTree.getFilters();
		if (type == JoinType.LEFTOUTER || type == JoinType.FULLOUTER) {
			joinTree.addFilterMapping(cond.getLeft(), cond.getRight(), filters
					.get(0).size());
		}
		if (type == JoinType.RIGHTOUTER || type == JoinType.FULLOUTER) {
			joinTree.addFilterMapping(cond.getRight(), cond.getLeft(), filters
					.get(1).size());
		}
	}

	private void parseJoinCondition(SparqlJoinTree joinTree, QGEdge edge,
			List<String> leftSrc, JoinType type) throws SemanticException {
		if (edge == null) {
			return;
		}

		ArrayList<String> leftCondAl1 = new ArrayList<String>();
		ArrayList<String> leftCondAl2 = new ArrayList<String>();
		parseJoinCondPopulateAlias(joinTree, edge.first.alias, leftCondAl1,
			leftCondAl2, null);

		ArrayList<String> rightCondAl1 = new ArrayList<String>();
		ArrayList<String> rightCondAl2 = new ArrayList<String>();
		parseJoinCondPopulateAlias(joinTree, edge.second.alias, leftCondAl1,
			leftCondAl2, null);

		if ((rightCondAl2.size() != 0)
				|| ((rightCondAl1.size() == 0) && (rightCondAl2.size() == 0))) {

			populateAliases(leftCondAl1, leftCondAl2, edge.first.alias,
				joinTree, leftSrc);
			populateAliases(rightCondAl1, rightCondAl2, edge.first.alias,
				joinTree, leftSrc);
			boolean nullsafe = false;
			joinTree.getNullSafes().add(nullsafe);
		}

	}

	@SuppressWarnings("nls")
	private void parseJoinCondPopulateAlias(SparqlJoinTree joinTree,
			String tableOrCol, ArrayList<String> leftAliases,
			ArrayList<String> rightAliases, ArrayList<String> fields)
			throws SemanticException {
		// String[] allAliases = joinTree.getAllAliases();

		// unparseTranslator.addIdentifierTranslation((ASTNode)
		if (isPresent(joinTree.getLeftAliases(), tableOrCol)) {
			if (!leftAliases.contains(tableOrCol)) {
				leftAliases.add(tableOrCol);
			}
		} else if (isPresent(joinTree.getRightAliases(), tableOrCol)) {
			if (!rightAliases.contains(tableOrCol)) {
				rightAliases.add(tableOrCol);
			}
		}
	}

	private boolean isPresent(String[] list, String elem) {
		for (String s : list) {
			if (s.toLowerCase().equals(elem)) {
				return true;
			}
		}

		return false;
	}

	private void populateAliases(List<String> leftAliases,
			List<String> rightAliases, String table, SparqlJoinTree joinTree,
			List<String> leftSrc) throws SemanticException {

		if (rightAliases.size() != 0) {
			assert rightAliases.size() == 1;
			// joinTree.getExpressions().get(1).add(condn);
		} else if (leftAliases.size() != 0) {
			// joinTree.getExpressions().get(0).add(condn);
			for (String s : leftAliases) {
				if (!leftSrc.contains(s)) {
					leftSrc.add(s);
				}
			}
		} else {
			// throw new SemanticException(ErrorMsg.INVALID_JOIN_CONDITION_2
			// .getMsg(condn));
		}
	}

	@SuppressWarnings("rawtypes")
	private Operator genJoinPlan(QueryGraph querygraph,
			Map<String, Operator> map) throws SemanticException {
		SparqlJoinTree joinTree = (SparqlJoinTree) querygraph.getQbJoinTree();
		Operator joinOp = genJoinOperator(querygraph, joinTree, map);
		return joinOp;
	}

	@SuppressWarnings("rawtypes")
	private Operator genJoinOperator(QueryGraph querygraph,
			SparqlJoinTree joinTree, Map<String, Operator> map)
			throws SemanticException {
		QBJoinTree leftChild = joinTree.getJoinSrc();
		Operator joinSrcOp = null;
		if (leftChild != null) {
			SparqlJoinTree tree = (SparqlJoinTree) leftChild;
			Operator joinOp = genJoinOperator(querygraph, tree, map);
			// ArrayList<ASTNode> filter =
			// joinTree.getFiltersForPushing().get(0);
			// for (ASTNode cond : filter) {
			// joinOp = genFilterPlan(querygraph, cond, joinOp);
			// }
			String src = joinTree.getJoinKeys().get(0)[0];
			joinSrcOp = genJoinReduceSinkChild(querygraph, joinTree, joinOp,
				src, 0);
		}

		Operator[] srcOps = new Operator[joinTree.getBaseSrc().length];

		HashSet<Integer> omitOpts = null; // set of input to the join that
											// should be
		// omitted by the output
		int pos = 0;
		for (String src : joinTree.getBaseSrc()) {
			if (src != null) {
				Operator srcOp = map.get(src.toLowerCase());

				// for left-semi join, generate an additional selection &
				// group-by
				// operator before ReduceSink
				// ArrayList<ASTNode> fields =
				// joinTree.getRHSSemijoinColumns(src);
				// if (fields != null) {
				// // the RHS table columns should be not be output from the
				// join
				// if (omitOpts == null) {
				// omitOpts = new HashSet<Integer>();
				// }
				// omitOpts.add(pos);
				//
				// // generate a selection operator for group-by keys only
				// srcOp = insertSelectForSemijoin(fields, srcOp);
				//
				// // generate a groupby operator (HASH mode) for a map-side
				// partial
				// // aggregation for semijoin
				// srcOp = genMapGroupByForSemijoin(qb, fields, srcOp,
				// GroupByDesc.Mode.HASH);
				// }

				// generate a ReduceSink operator for the join
				srcOps[pos] = genJoinReduceSinkChild(querygraph, joinTree,
					srcOp, src, pos);
				pos++;

			} else {
				assert pos == 0;
				srcOps[pos++] = null;
			}
		}

		// Type checking and implicit type conversion for join keys
		genJoinOperatorTypeCheck(joinSrcOp, srcOps);

		JoinOperator joinOp = (JoinOperator) genJoinOperatorChildren(joinTree,
			joinSrcOp, srcOps, omitOpts);
		joinContext.put(joinOp, joinTree);
		return joinOp;
	}

	@SuppressWarnings({ "nls", "rawtypes" })
	private Operator genJoinReduceSinkChild(QueryGraph querygraph,
			SparqlJoinTree joinTree, Operator child, String srcName, int pos)
			throws SemanticException {
		RowResolver inputRS = opParseCtx.get(child).getRowResolver();
		RowResolver outputRS = new RowResolver();
		ArrayList<String> outputColumns = new ArrayList<String>();
		ArrayList<ExprNodeDesc> reduceKeys = new ArrayList<ExprNodeDesc>();

		// Compute join keys and store in reduceKeys
		// ArrayList<ASTNode> exprs = joinTree.getExpressions().get(pos);
		// for (int i = 0; i < exprs.size(); i++) {
		// ASTNode expr = exprs.get(i);
		// reduceKeys.add(genExprNodeDesc(expr, inputRS));
		// }
		String[] joinKey = null;

		for (String jk[] : joinTree.getJoinKeys()) {
			if (jk[0].equalsIgnoreCase(srcName)) {
				joinKey = jk;
			}
		}

		reduceKeys.add(genJoinNodeDesc(joinKey, inputRS));

		// Walk over the input row resolver and copy in the output
		ArrayList<ExprNodeDesc> reduceValues = new ArrayList<ExprNodeDesc>();
		Iterator<String> tblNamesIter = inputRS.getTableNames().iterator();
		Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
		while (tblNamesIter.hasNext()) {
			String src = tblNamesIter.next();
			HashMap<String, ColumnInfo> fMap = inputRS.getFieldMap(src);
			for (Map.Entry<String, ColumnInfo> entry : fMap.entrySet()) {
				String field = entry.getKey();
				ColumnInfo valueInfo = entry.getValue();
				ExprNodeColumnDesc inputExpr = new ExprNodeColumnDesc(
						valueInfo.getType(), valueInfo.getInternalName(),
						valueInfo.getTabAlias(), valueInfo.getIsVirtualCol());
				reduceValues.add(inputExpr);
				if (outputRS.get(src, field) == null) {
					String col = getColumnInternalName(reduceValues.size() - 1);
					outputColumns.add(col);
					ColumnInfo newColInfo = new ColumnInfo(
							Utilities.ReduceField.VALUE.toString() + "." + col,
							valueInfo.getType(), src,
							valueInfo.getIsVirtualCol(),
							valueInfo.isHiddenVirtualCol());

					colExprMap.put(newColInfo.getInternalName(), inputExpr);
					outputRS.put(src, field, newColInfo);
				}
			}
		}

		int numReds = -1;

		// Use only 1 reducer in case of cartesian product
		if (reduceKeys.size() == 0) {
			numReds = 1;

			// Cartesian product is not supported in strict mode
			if (conf.getVar(HiveConf.ConfVars.HIVEMAPREDMODE).equalsIgnoreCase(
				"strict")) {
				throw new SemanticException(
						ErrorMsg.NO_CARTESIAN_PRODUCT.getMsg());
			}
		}

		ReduceSinkOperator rsOp = (ReduceSinkOperator) putOpInsertMap(
			OperatorFactory.getAndMakeChild(PlanUtils.getReduceSinkDesc(
				reduceKeys, reduceValues, outputColumns, false,
				joinTree.getNextTag(), reduceKeys.size(), numReds),
				new RowSchema(outputRS.getColumnInfos()), child), outputRS);
		rsOp.setColumnExprMap(colExprMap);
		return rsOp;
	}

	/**
	 * Generates an expression node descriptor for the expression passed in the
	 * arguments. This function uses the row resolver and the metadata
	 * information that are passed as arguments to resolve the column names to
	 * internal names.
	 *
	 * @param node
	 *            The expression
	 * @param input
	 *            The row resolver
	 * @return exprNodeDesc
	 * @throws SemanticException
	 */
	public ExprNodeDesc genNodeDesc(String[] node, RowResolver input)
			throws SemanticException {
		// Since the user didn't supply a customized type-checking context,
		// use default settings.
		TypeCheckCtx tcCtx = new TypeCheckCtx(input);
		return genNodeDesc(node, input, tcCtx);
	}

	/**
	 * Generates an expression node descriptor for the expression passed in the
	 * arguments. This function uses the row resolver and the metadata
	 * information that are passed as arguments to resolve the column names to
	 * internal names.
	 *
	 * @param node
	 *            The expression
	 * @param input
	 *            The row resolver
	 * @param tcCtx
	 *            Customized type-checking context
	 * @return exprNodeDesc
	 * @throws SemanticException
	 */
	public ExprNodeDesc genNodeDesc(String[] node, RowResolver input,
			TypeCheckCtx tcCtx) throws SemanticException {

		// Create the walker and the rules dispatcher.
		tcCtx.setUnparseTranslator(unparseTranslator);

		ExprNodeDesc desc = TypeCheckProcFactory.genSelectNodeDesc(node, tcCtx);

		if (desc == null) {
			String errMsg = tcCtx.getError();
			if (errMsg == null) {
				errMsg = "Error in parsing ";
			}
			throw new SemanticException(errMsg);
		}

		if (!unparseTranslator.isEnabled()) {
			// Not creating a view, so no need to track view expansions.
			return desc;
		}

		return desc;
	}

	public ExprNodeDesc genJoinNodeDesc(String[] node, RowResolver input)
			throws SemanticException {
		// Since the user didn't supply a customized type-checking context,
		// use default settings.
		TypeCheckCtx tcCtx = new TypeCheckCtx(input);

		// Create the walker and the rules dispatcher.
		tcCtx.setUnparseTranslator(unparseTranslator);

		// HashMap<Node, Object> nodeOutputs = TypeCheckProcFactory.genExprNode(
		// expr, tcCtx);
		// ExprNodeDesc desc = (ExprNodeDesc) nodeOutputs.get(expr);

		ExprNodeDesc desc = TypeCheckProcFactory.genJoinNodeDesc(node, tcCtx);

		if (desc == null) {
			String errMsg = tcCtx.getError();
			if (errMsg == null) {
				errMsg = "Error in parsing ";
			}
			throw new SemanticException(errMsg);
		}

		if (!unparseTranslator.isEnabled()) {
			// Not creating a view, so no need to track view expansions.
			return desc;
		}

		return desc;
	}

	public ExprNodeDesc genFilterNodeDesc(QueryGraph querygraph,
			RowResolver input) throws SemanticException {
		// Since the user didn't supply a customized type-checking context,
		// use default settings.
		TypeCheckCtx tcCtx = new TypeCheckCtx(input);

		// Create the walker and the rules dispatcher.
		tcCtx.setUnparseTranslator(unparseTranslator);

		// HashMap<Node, Object> nodeOutputs = TypeCheckProcFactory.genExprNode(
		// expr, tcCtx);
		// ExprNodeDesc desc = (ExprNodeDesc) nodeOutputs.get(expr);

		ExprNodeDesc desc = TypeCheckProcFactory.genFilterNodeDesc(querygraph,
			tcCtx);

		if (desc == null) {
			String errMsg = tcCtx.getError();
			if (errMsg == null) {
				errMsg = "Error in parsing ";
			}
			throw new SemanticException(errMsg);
		}

		if (!unparseTranslator.isEnabled()) {
			// Not creating a view, so no need to track view expansions.
			return desc;
		}

		return desc;
	}

	@SuppressWarnings("rawtypes")
	private void genJoinOperatorTypeCheck(Operator left, Operator[] right)
			throws SemanticException {
		// keys[i] -> ArrayList<exprNodeDesc> for the i-th join operator key
		// list
		ArrayList<ArrayList<ExprNodeDesc>> keys = new ArrayList<ArrayList<ExprNodeDesc>>();
		int keyLength = 0;
		for (int i = 0; i < right.length; i++) {
			Operator oi = (i == 0 && right[i] == null ? left : right[i]);
			ReduceSinkDesc now = ((ReduceSinkOperator) (oi)).getConf();
			if (i == 0) {
				keyLength = now.getKeyCols().size();
			} else {
				assert (keyLength == now.getKeyCols().size());
			}
			keys.add(now.getKeyCols());
		}
		// implicit type conversion hierarchy
		for (int k = 0; k < keyLength; k++) {
			// Find the common class for type conversion
			TypeInfo commonType = keys.get(0).get(k).getTypeInfo();
			for (int i = 1; i < right.length; i++) {
				TypeInfo a = commonType;
				TypeInfo b = keys.get(i).get(k).getTypeInfo();
				commonType = FunctionRegistry.getCommonClassForComparison(a, b);
				if (commonType == null) {
					throw new SemanticException(
							"Cannot do equality join on different types: "
									+ a.getTypeName() + " and "
									+ b.getTypeName());
				}
			}
			// Add implicit type conversion if necessary
			for (int i = 0; i < right.length; i++) {
				if (!commonType.equals(keys.get(i).get(k).getTypeInfo())) {
					keys.get(i).set(
						k,
						TypeCheckProcFactory.DefaultExprProcessor
								.getFuncExprNodeDesc(commonType.getTypeName(),
									keys.get(i).get(k)));
				}
			}
		}
		// regenerate keySerializationInfo because the ReduceSinkOperator's
		// output key types might have changed.
		for (int i = 0; i < right.length; i++) {
			Operator oi = (i == 0 && right[i] == null ? left : right[i]);
			ReduceSinkDesc now = ((ReduceSinkOperator) (oi)).getConf();

			now.setKeySerializeInfo(PlanUtils.getReduceKeyTableDesc(
				PlanUtils.getFieldSchemasFromColumnList(now.getKeyCols(),
					"joinkey"), now.getOrder()));
		}
	}

	@SuppressWarnings("rawtypes")
	private Operator genJoinOperatorChildren(SparqlJoinTree join,
			Operator left, Operator[] right, HashSet<Integer> omitOpts)
			throws SemanticException {

		RowResolver outputRS = new RowResolver();
		ArrayList<String> outputColumnNames = new ArrayList<String>();
		// all children are base classes
		Operator<?>[] rightOps = new Operator[right.length];
		int outputPos = 0;

		Map<String, Byte> reversedExprs = new HashMap<String, Byte>();
		HashMap<Byte, List<ExprNodeDesc>> exprMap = new HashMap<Byte, List<ExprNodeDesc>>();
		Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
		HashMap<Integer, Set<String>> posToAliasMap = new HashMap<Integer, Set<String>>();
		HashMap<Byte, List<ExprNodeDesc>> filterMap = new HashMap<Byte, List<ExprNodeDesc>>();

		for (int pos = 0; pos < right.length; ++pos) {

			Operator input = right[pos];
			if (input == null) {
				input = left;
			}

			ArrayList<ExprNodeDesc> keyDesc = new ArrayList<ExprNodeDesc>();
			ArrayList<ExprNodeDesc> filterDesc = new ArrayList<ExprNodeDesc>();
			Byte tag = Byte
					.valueOf((byte) (((ReduceSinkDesc) (input.getConf()))
							.getTag()));

			// check whether this input operator produces output
			if (omitOpts == null || !omitOpts.contains(pos)) {
				// prepare output descriptors for the input opt
				RowResolver inputRS = opParseCtx.get(input).getRowResolver();
				Iterator<String> keysIter = inputRS.getTableNames().iterator();
				Set<String> aliases = posToAliasMap.get(pos);
				if (aliases == null) {
					aliases = new HashSet<String>();
					posToAliasMap.put(pos, aliases);
				}
				while (keysIter.hasNext()) {
					String key = keysIter.next();
					aliases.add(key);
					HashMap<String, ColumnInfo> map = inputRS.getFieldMap(key);
					Iterator<String> fNamesIter = map.keySet().iterator();
					while (fNamesIter.hasNext()) {
						String field = fNamesIter.next();
						ColumnInfo valueInfo = inputRS.get(key, field);
						keyDesc.add(new ExprNodeColumnDesc(valueInfo.getType(),
								valueInfo.getInternalName(), valueInfo
										.getTabAlias(), valueInfo
										.getIsVirtualCol()));

						if (outputRS.get(key, field) == null) {
							String colName = getColumnInternalName(outputPos);
							outputPos++;
							outputColumnNames.add(colName);
							colExprMap.put(colName,
								keyDesc.get(keyDesc.size() - 1));
							outputRS.put(key, field,
								new ColumnInfo(colName, valueInfo.getType(),
										key, valueInfo.getIsVirtualCol(),
										valueInfo.isHiddenVirtualCol()));
							reversedExprs.put(colName, tag);
						}
					}
				}

			}
			exprMap.put(tag, keyDesc);
			filterMap.put(tag, filterDesc);
			rightOps[pos] = input;
		}

		JoinCondDesc[] joinCondns = new JoinCondDesc[join.getJoinCond().length];
		for (int i = 0; i < join.getJoinCond().length; i++) {
			JoinCond condn = join.getJoinCond()[i];
			joinCondns[i] = new JoinCondDesc(condn);
		}

		JoinDesc desc = new JoinDesc(exprMap, outputColumnNames,
				join.getNoOuterJoin(), joinCondns, filterMap);
		desc.setReversedExprs(reversedExprs);
		desc.setFilterMap(join.getFilterMap());

		JoinOperator joinOp = (JoinOperator) OperatorFactory.getAndMakeChild(
			desc, new RowSchema(outputRS.getColumnInfos()), rightOps);
		joinOp.setColumnExprMap(colExprMap);
		joinOp.setPosToAliasMap(posToAliasMap);

		if (join.getNullSafes() != null) {
			boolean[] nullsafes = new boolean[join.getNullSafes().size()];
			for (int i = 0; i < nullsafes.length; i++) {
				nullsafes[i] = join.getNullSafes().get(i);
			}
			desc.setNullSafes(nullsafes);
		}
		return putOpInsertMap(joinOp, outputRS);
	}

	@SuppressWarnings({ "nls", "rawtypes" })
	private Operator genBodyPlan(QueryGraph querygraph, Operator input)
			throws SemanticException {
		QBParseInfo qbp = querygraph.getParseInfo();

		TreeSet<String> ks = new TreeSet<String>(qbp.getClauseNames());
		// For multi-group by with the same distinct, we ignore all user hints
		// currently. It doesnt matter whether he has asked to do
		// map-side aggregation or not. Map side aggregation is turned off
		// List<ASTNode> commonDistinctExprs =
		// getCommonDistinctExprs(querygraph,
		// input);
		// List<ASTNode> commonDistinctExprs = null;

		// Consider a query like:
		//
		// from src
		// insert overwrite table dest1 select col1, count(distinct colx) group
		// by col1
		// insert overwrite table dest2 select col2, count(distinct colx) group
		// by col2;
		//
		// With HIVE_OPTIMIZE_MULTI_GROUPBY_COMMON_DISTINCTS set to true, first
		// we spray by the distinct
		// value (colx), and then perform the 2 groups bys. This makes sense if
		// map-side aggregation is
		// turned off. However, with maps-side aggregation, it might be useful
		// in some cases to treat
		// the 2 inserts independently, thereby performing the query above in
		// 2MR jobs instead of 3
		// (due to spraying by distinct key first).
		// boolean optimizeMultiGroupBy = commonDistinctExprs != null
		// &&
		// conf.getBoolVar(HiveConf.ConfVars.HIVE_OPTIMIZE_MULTI_GROUPBY_COMMON_DISTINCTS);

		Operator curr = input;

		// if there is a single distinct, optimize that. Spray initially by the
		// distinct key,
		// no computation at the mapper. Have multiple group by operators at the
		// reducer - and then
		// proceed
		// if (optimizeMultiGroupBy) {
		// curr = createCommonReduceSink(querygraph, input);
		//
		// RowResolver currRR = opParseCtx.get(curr).getRowResolver();
		// // create a forward operator
		// input = putOpInsertMap(OperatorFactory.getAndMakeChild(
		// new ForwardDesc(), new RowSchema(currRR.getColumnInfos()),
		// curr), currRR);
		//
		// for (String dest : ks) {
		// curr = input;
		// curr = genGroupByPlan2MRMultiGroupBy(dest, querygraph, curr);
		// curr = genSelectPlan(dest, querygraph, curr);
		// Integer limit = qbp.getDestLimit(dest);
		// if (limit != null) {
		// curr = genLimitMapRedPlan(dest, qb, curr, limit.intValue(),
		// true);
		// qb.getParseInfo().setOuterQueryLimit(limit.intValue());
		// }
		// curr = genFileSinkPlan(dest, qb, curr);
		// }
		// } else {
		List<List<String>> commonGroupByDestGroups = null;

		// If we can put multiple group bys in a single reducer, determine
		// suitable groups of
		// expressions, otherwise treat all the expressions as a single
		// group
		if (conf.getBoolVar(HiveConf.ConfVars.HIVEMULTIGROUPBYSINGLEREDUCER)) {
			try {
				commonGroupByDestGroups = getCommonGroupByDestGroups(
					querygraph, curr);
			} catch (SemanticException e) {
				LOG.error("Failed to group clauses by common spray keys.", e);
			}
		}

		if (commonGroupByDestGroups == null) {
			commonGroupByDestGroups = new ArrayList<List<String>>();
			commonGroupByDestGroups.add(new ArrayList<String>(ks));
		}

		if (!commonGroupByDestGroups.isEmpty()) {

			// Iterate over each group of subqueries with the same group
			// by/distinct keys
			for (List<String> commonGroupByDestGroup : commonGroupByDestGroups) {
				if (commonGroupByDestGroup.isEmpty()) {
					continue;
				}

				String firstDest = commonGroupByDestGroup.get(0);
				// Constructs a standard group by plan if:
				// There is no other subquery with the same group
				// by/distinct keys or
				// (There are no aggregations in a representative query for
				// the group and
				// There is no group by in that representative query) or
				// The data is skewed or
				// The conf variable used to control combining group bys
				// into a single reducer is false
				if (commonGroupByDestGroup.size() == 1
						|| (qbp.getAggregationExprsForClause(firstDest).size() == 0 && getGroupByForClause(
							qbp, firstDest).size() == 0)
						|| conf.getBoolVar(HiveConf.ConfVars.HIVEGROUPBYSKEW)
						|| !conf.getBoolVar(HiveConf.ConfVars.HIVEMULTIGROUPBYSINGLEREDUCER)) {

					// Go over all the destination tables
					for (String dest : commonGroupByDestGroup) {
						curr = input;

						if (qbp.getWhrForClause(dest) != null) {
							curr = genFilterPlan(dest, querygraph, curr);
						}

						// if (qbp.getAggregationExprsForClause(dest).size()
						// != 0
						// || getGroupByForClause(qbp, dest).size() > 0) {
						// // multiple distincts is not supported with
						// skew
						// // in data
						// if
						// (conf.getBoolVar(HiveConf.ConfVars.HIVEGROUPBYSKEW)
						// && qbp.getDistinctFuncExprsForClause(
						// dest).size() > 1) {
						// throw new SemanticException(
						// ErrorMsg.UNSUPPORTED_MULTIPLE_DISTINCTS
						// .getMsg());
						// }
						// // insert a select operator here used by the
						// // ColumnPruner to reduce
						// // the data to shuffle
						// curr = insertSelectAllPlanForGroupBy(curr);
						// if
						// (conf.getBoolVar(HiveConf.ConfVars.HIVEMAPSIDEAGGREGATE))
						// {
						// if (!conf
						// .getBoolVar(HiveConf.ConfVars.HIVEGROUPBYSKEW))
						// {
						// curr = genGroupByPlanMapAggrNoSkew(
						// dest, qb, curr);
						// } else {
						// curr = genGroupByPlanMapAggr2MR(dest,
						// qb, curr);
						// }
						// } else if (conf
						// .getBoolVar(HiveConf.ConfVars.HIVEGROUPBYSKEW))
						// {
						// curr = genGroupByPlan2MR(dest, qb, curr);
						// } else {
						// curr = genGroupByPlan1MR(dest, qb, curr);
						// }
						// }

						curr = genPostGroupByBodyPlan(curr, dest, querygraph);
					}
				} else {
					// curr = genGroupByPlan1MRMultiReduceGB(
					// commonGroupByDestGroup, querygraph, input);
				}
			}
		}
		// }

		if (LOG.isDebugEnabled()) {
			LOG.debug("Created Body Plan for Query Block " + querygraph.getId());
		}

		return curr;
	}

	@SuppressWarnings({ "nls", "rawtypes" })
	private Operator genFilterPlan(String dest, QueryGraph querygraph,
			Operator input) throws SemanticException {

		ASTNode whereExpr = querygraph.getParseInfo().getWhrForClause(dest);
		return genFilterPlan(querygraph, whereExpr, input);
	}

	/**
	 * create a filter plan. The condition and the inputs are specified.
	 *
	 * @param querygraph
	 *            current query block
	 * @param condn
	 *            The condition to be resolved
	 * @param input
	 *            the input operator
	 */
	@SuppressWarnings({ "nls", "rawtypes" })
	private Operator genFilterPlan(QueryGraph querygraph, ASTNode condn,
			Operator input) throws SemanticException {

		OpParseContext inputCtx = opParseCtx.get(input);
		RowResolver inputRR = inputCtx.getRowResolver();
		Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
			new FilterDesc(genFilterNodeDesc(querygraph, inputRR), false),
			new RowSchema(inputRR.getColumnInfos()), input), inputRR);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Created Filter Plan for " + querygraph.getId()
					+ " row schema: " + inputRR.toString());
		}
		return output;
	}

	// Groups the clause names into lists so that any two clauses in the same
	// list has the same
	// group by and distinct keys and no clause appears in more than one list.
	// Returns a list of the
	// lists of clauses.
	@SuppressWarnings("rawtypes")
	private List<List<String>> getCommonGroupByDestGroups(
			QueryGraph querygraph, Operator input) throws SemanticException {

		// RowResolver inputRR = opParseCtx.get(input).getRowResolver();
		QBParseInfo qbp = querygraph.getParseInfo();

		TreeSet<String> ks = new TreeSet<String>();
		ks.addAll(qbp.getClauseNames());

		List<List<String>> commonGroupByDestGroups = new ArrayList<List<String>>();

		// If this is a trivial query block return
		if (ks.size() <= 1) {
			List<String> oneList = new ArrayList<String>(1);
			if (ks.size() == 1) {
				oneList.add(ks.first());
			}
			commonGroupByDestGroups.add(oneList);
			return commonGroupByDestGroups;
		}

		// List<List<ExprNodeDesc.ExprNodeDescEqualityWrapper>> sprayKeyLists =
		// new
		// ArrayList<List<ExprNodeDesc.ExprNodeDescEqualityWrapper>>(ks.size());
		//
		// // Iterate over each clause
		// for (String dest : ks) {
		//
		// List<ExprNodeDesc.ExprNodeDescEqualityWrapper> sprayKeys =
		// getDistinctExprs(qbp, dest, inputRR);
		//
		// // Add the group by expressions
		// List<ASTNode> grpByExprs = getGroupByForClause(qbp, dest);
		// for (ASTNode grpByExpr : grpByExprs) {
		// ExprNodeDesc.ExprNodeDescEqualityWrapper grpByExprWrapper =
		// new
		// ExprNodeDesc.ExprNodeDescEqualityWrapper(genExprNodeDesc(grpByExpr,
		// inputRR));
		// if (!sprayKeys.contains(grpByExprWrapper)) {
		// sprayKeys.add(grpByExprWrapper);
		// }
		// }
		//
		// // Loop through each of the lists of exprs, looking for a match
		// boolean found = false;
		// for (int i = 0; i < sprayKeyLists.size(); i++) {
		//
		// if (!matchExprLists(sprayKeyLists.get(i), sprayKeys)) {
		// continue;
		// }
		//
		// // A match was found, so add the clause to the corresponding list
		// commonGroupByDestGroups.get(i).add(dest);
		// found = true;
		// break;
		// }
		//
		// // No match was found, so create new entries
		// if (!found) {
		// sprayKeyLists.add(sprayKeys);
		// List<String> destGroup = new ArrayList<String>();
		// destGroup.add(dest);
		// commonGroupByDestGroups.add(destGroup);
		// }
		// }
		//
		return commonGroupByDestGroups;
	}

	@SuppressWarnings("rawtypes")
	private Operator genPostGroupByBodyPlan(Operator curr, String dest,
			QueryGraph querygraph) throws SemanticException {

		QBParseInfo qbp = querygraph.getParseInfo();

		// Insert HAVING plan here
		// if (qbp.getHavingForClause(dest) != null) {
		// if (getGroupByForClause(qbp, dest).size() == 0) {
		// throw new SemanticException("HAVING specified without GROUP BY");
		// }
		// curr = genHavingPlan(dest, qb, curr);
		// }

		curr = genSelectPlan(dest, querygraph, curr);
		// Integer limit = qbp.getDestLimit(dest);

		// Expressions are not supported currently without a alias.

		// Reduce sink is needed if the query contains a cluster by, distribute
		// by,
		// order by or a sort by clause.
		boolean genReduceSink = false;

		// Currently, expressions are not allowed in cluster by, distribute by,
		// order by or a sort by clause. For each of the above clause types,
		// check
		// if the clause contains any expression.
		if (qbp.getClusterByForClause(dest) != null) {
			genReduceSink = true;
		}

		if (qbp.getDistributeByForClause(dest) != null) {
			genReduceSink = true;
		}

		if (qbp.getOrderByForClause(dest) != null) {
			genReduceSink = true;
		}

		if (qbp.getSortByForClause(dest) != null) {
			genReduceSink = true;
		}

		if (genReduceSink) {
			// int numReducers = -1;
			//
			// // Use only 1 reducer if order by is present
			// if (qbp.getOrderByForClause(dest) != null) {
			// numReducers = 1;
			// }
			//
			// curr = genReduceSinkPlan(dest, querygraph, curr, numReducers);
		}

		if (qbp.getIsSubQ()) {
			// if (limit != null) {
			// // In case of order by, only 1 reducer is used, so no need of
			// // another shuffle
			// curr = genLimitMapRedPlan(dest, qb, curr, limit.intValue(),
			// qbp.getOrderByForClause(dest) != null ? false : true);
			// }
		} else {
			curr = genConversionOps(dest, querygraph, curr);
			// exact limit can be taken care of by the fetch operator
			// if (limit != null) {
			// boolean extraMRStep = true;
			//
			// if (qb.getIsQuery() && qbp.getClusterByForClause(dest) == null
			// && qbp.getSortByForClause(dest) == null) {
			// extraMRStep = false;
			// }
			//
			// curr = genLimitMapRedPlan(dest, qb, curr, limit.intValue(),
			// extraMRStep);
			// qb.getParseInfo().setOuterQueryLimit(limit.intValue());
			// }
			curr = genFileSinkPlan(dest, querygraph, curr);
		}

		// change curr ops row resolver's tab aliases to query alias if it
		// exists
		if (querygraph.getParseInfo().getAlias() != null) {
			RowResolver rr = opParseCtx.get(curr).getRowResolver();
			RowResolver newRR = new RowResolver();
			String alias = querygraph.getParseInfo().getAlias();
			for (ColumnInfo colInfo : rr.getColumnInfos()) {
				String name = colInfo.getInternalName();
				String[] tmp = rr.reverseLookup(name);
				newRR.put(alias, tmp[1], colInfo);
			}
			opParseCtx.get(curr).setRowResolver(newRR);
		}

		return curr;
	}

	/**
	 * Check for HOLD_DDLTIME hint.
	 *
	 * @param querygraph
	 * @return true if HOLD_DDLTIME is set, false otherwise.
	 */
	private boolean checkHoldDDLTime(QueryGraph querygraph) {
		ASTNode hints = querygraph.getParseInfo().getHints();
		if (hints == null) {
			return false;
		}
		for (int pos = 0; pos < hints.getChildCount(); pos++) {
			ASTNode hint = (ASTNode) hints.getChild(pos);
			if (((ASTNode) hint.getChild(0)).getToken().getType() == HiveParser.TOK_HOLD_DDLTIME) {
				return true;
			}
		}
		return false;
	}

	@SuppressWarnings({ "nls", "rawtypes" })
	private Operator genFileSinkPlan(String dest, QueryGraph querygraph,
			Operator input) throws SemanticException {

		RowResolver inputRR = opParseCtx.get(input).getRowResolver();
		QBMetaData qbm = querygraph.getMetaData();
		Integer dest_type = qbm.getDestTypeForAlias(dest);

		Table dest_tab = null; // destination table if any
		Partition dest_part = null;// destination partition if any
		String queryTmpdir = null; // the intermediate destination directory
		Path dest_path = null; // the final destination directory
		TableDesc table_desc = null;
		int currentTableId = 0;
		boolean isLocal = false;
		SortBucketRSCtx rsCtx = new SortBucketRSCtx();
		DynamicPartitionCtx dpCtx = null;
		LoadTableDesc ltd = null;
		boolean holdDDLTime = checkHoldDDLTime(querygraph);
		ListBucketingCtx lbCtx = null;

		switch (dest_type.intValue()) {
		case QBMetaData.DEST_TABLE: {

			dest_tab = qbm.getDestTableForAlias(dest);

			// Is the user trying to insert into a external tables
			if ((!conf
					.getBoolVar(HiveConf.ConfVars.HIVE_INSERT_INTO_EXTERNAL_TABLES))
					&& (dest_tab.getTableType()
							.equals(TableType.EXTERNAL_TABLE))) {
				throw new SemanticException(
						ErrorMsg.INSERT_EXTERNAL_TABLE.getMsg(dest_tab
								.getTableName()));
			}

			Map<String, String> partSpec = qbm.getPartSpecForAlias(dest);
			dest_path = dest_tab.getPath();

			// check for partition
			List<FieldSchema> parts = dest_tab.getPartitionKeys();
			if (parts != null && parts.size() > 0) { // table is partitioned
				if (partSpec == null || partSpec.size() == 0) { // user did NOT
																// specify
																// partition
					throw new SemanticException(generateErrorMessage(querygraph
							.getParseInfo().getDestForClause(dest),
						ErrorMsg.NEED_PARTITION_ERROR.getMsg()));
				}
				// the HOLD_DDLTIIME hint should not be used with dynamic
				// partition since the
				// newly generated partitions should always update their DDLTIME
				if (holdDDLTime) {
					throw new SemanticException(generateErrorMessage(querygraph
							.getParseInfo().getDestForClause(dest),
						ErrorMsg.HOLD_DDLTIME_ON_NONEXIST_PARTITIONS.getMsg()));
				}
				dpCtx = qbm.getDPCtx(dest);
				if (dpCtx == null) {
					Utilities.validatePartSpec(dest_tab, partSpec);
					dpCtx = new DynamicPartitionCtx(
							dest_tab,
							partSpec,
							conf.getVar(HiveConf.ConfVars.DEFAULTPARTITIONNAME),
							conf.getIntVar(HiveConf.ConfVars.DYNAMICPARTITIONMAXPARTSPERNODE));
					qbm.setDPCtx(dest, dpCtx);
				}

				if (HiveConf.getBoolVar(conf,
					HiveConf.ConfVars.DYNAMICPARTITIONING)) { // allow
																// DP
					if (dpCtx.getNumDPCols() > 0
							&& (HiveConf.getBoolVar(conf,
								HiveConf.ConfVars.HIVEMERGEMAPFILES) || HiveConf
									.getBoolVar(conf,
										HiveConf.ConfVars.HIVEMERGEMAPREDFILES))
							&& Utilities.supportCombineFileInputFormat() == false) {
						// Do not support merge for Hadoop versions (pre-0.20)
						// that do not
						// support CombineHiveInputFormat
						HiveConf.setBoolVar(conf,
							HiveConf.ConfVars.HIVEMERGEMAPFILES, false);
						HiveConf.setBoolVar(conf,
							HiveConf.ConfVars.HIVEMERGEMAPREDFILES, false);
					}
					// turn on hive.task.progress to update # of partitions
					// created to the JT
					HiveConf.setBoolVar(conf,
						HiveConf.ConfVars.HIVEJOBPROGRESS, true);

				} else { // QBMetaData.DEST_PARTITION capture the all-SP case
					throw new SemanticException(generateErrorMessage(querygraph
							.getParseInfo().getDestForClause(dest),
						ErrorMsg.DYNAMIC_PARTITION_DISABLED.getMsg()));
				}
				if (dpCtx.getSPPath() != null) {
					dest_path = new Path(dest_tab.getPath(), dpCtx.getSPPath());
				}
				if ((dest_tab.getNumBuckets() > 0)
						&& (conf.getBoolVar(HiveConf.ConfVars.HIVEENFORCEBUCKETING))) {
					dpCtx.setNumBuckets(dest_tab.getNumBuckets());
				}
			}

			boolean isNonNativeTable = dest_tab.isNonNative();
			if (isNonNativeTable) {
				queryTmpdir = dest_path.toUri().getPath();
			} else {
				queryTmpdir = ctx.getExternalTmpFileURI(dest_path.toUri());
			}
			if (dpCtx != null) {
				// set the root of the temporay path where dynamic partition
				// columns will populate
				dpCtx.setRootPath(queryTmpdir);
			}
			// this table_desc does not contain the partitioning columns
			table_desc = Utilities.getTableDesc(dest_tab);

			// Add sorting/bucketing if needed
			input = genBucketingSortingDest(dest, input, querygraph,
				table_desc, dest_tab, rsCtx);

			idToTableNameMap.put(String.valueOf(destTableId),
				dest_tab.getTableName());
			currentTableId = destTableId;
			destTableId++;

			lbCtx = constructListBucketingCtx(dest_tab.getSkewedColNames(),
				dest_tab.getSkewedColValues(),
				dest_tab.getSkewedColValueLocationMaps(),
				dest_tab.isStoredAsSubDirectories(), conf);

			// Create the work for moving the table
			// NOTE: specify Dynamic partitions in dest_tab for WriteEntity
			if (!isNonNativeTable) {
				ltd = new LoadTableDesc(queryTmpdir,
						ctx.getExternalTmpFileURI(dest_path.toUri()),
						table_desc, dpCtx);
				ltd.setReplace(!querygraph.getParseInfo().isInsertIntoTable(
					dest_tab.getDbName(), dest_tab.getTableName()));
				ltd.setLbCtx(lbCtx);

				if (holdDDLTime) {
					LOG.info("this query will not update transient_lastDdlTime!");
					ltd.setHoldDDLTime(true);
				}
				loadTableWork.add(ltd);
			}

			WriteEntity output = null;

			// Here only register the whole table for post-exec hook if no DP
			// present
			// in the case of DP, we will register WriteEntity in MoveTask when
			// the
			// list of dynamically created partitions are known.
			if ((dpCtx == null || dpCtx.getNumDPCols() == 0)) {
				output = new WriteEntity(dest_tab);
				if (!outputs.add(output)) {
					throw new SemanticException(
							ErrorMsg.OUTPUT_SPECIFIED_MULTIPLE_TIMES
									.getMsg(dest_tab.getTableName()));
				}
			}
			if ((dpCtx != null) && (dpCtx.getNumDPCols() >= 0)) {
				// No static partition specified
				if (dpCtx.getNumSPCols() == 0) {
					output = new WriteEntity(dest_tab, false);
					outputs.add(output);
				}
				// part of the partition specified
				// Create a DummyPartition in this case. Since, the metastore
				// does not store partial
				// partitions currently, we need to store dummy partitions
				else {
					try {
						String ppath = dpCtx.getSPPath();
						ppath = ppath.substring(0, ppath.length() - 1);
						DummyPartition p = new DummyPartition(
								dest_tab,
								dest_tab.getDbName() + "@"
										+ dest_tab.getTableName() + "@" + ppath,
								partSpec);
						output = new WriteEntity(p, false);
						outputs.add(output);
					} catch (HiveException e) {
						throw new SemanticException(e.getMessage(), e);
					}
				}
			}

			ctx.getLoadTableOutputMap().put(ltd, output);
			break;
		}
		case QBMetaData.DEST_PARTITION: {

			dest_part = qbm.getDestPartitionForAlias(dest);
			dest_tab = dest_part.getTable();
			if ((!conf
					.getBoolVar(HiveConf.ConfVars.HIVE_INSERT_INTO_EXTERNAL_TABLES))
					&& dest_tab.getTableType().equals(TableType.EXTERNAL_TABLE)) {
				throw new SemanticException(
						ErrorMsg.INSERT_EXTERNAL_TABLE.getMsg(dest_tab
								.getTableName()));
			}

			Path tabPath = dest_tab.getPath();
			Path partPath = dest_part.getPartitionPath();

			// if the table is in a different dfs than the partition,
			// replace the partition's dfs with the table's dfs.
			dest_path = new Path(tabPath.toUri().getScheme(), tabPath.toUri()
					.getAuthority(), partPath.toUri().getPath());

			queryTmpdir = ctx.getExternalTmpFileURI(dest_path.toUri());
			table_desc = Utilities.getTableDesc(dest_tab);

			// Add sorting/bucketing if needed
			input = genBucketingSortingDest(dest, input, querygraph,
				table_desc, dest_tab, rsCtx);

			idToTableNameMap.put(String.valueOf(destTableId),
				dest_tab.getTableName());
			currentTableId = destTableId;
			destTableId++;

			lbCtx = constructListBucketingCtx(dest_part.getSkewedColNames(),
				dest_part.getSkewedColValues(),
				dest_part.getSkewedColValueLocationMaps(),
				dest_part.isStoredAsSubDirectories(), conf);
			ltd = new LoadTableDesc(queryTmpdir,
					ctx.getExternalTmpFileURI(dest_path.toUri()), table_desc,
					dest_part.getSpec());
			ltd.setReplace(!querygraph.getParseInfo().isInsertIntoTable(
				dest_tab.getDbName(), dest_tab.getTableName()));
			ltd.setLbCtx(lbCtx);

			if (holdDDLTime) {
				try {
					Partition part = db.getPartition(dest_tab,
						dest_part.getSpec(), false);
					if (part == null) {
						throw new SemanticException(generateErrorMessage(
							querygraph.getParseInfo().getDestForClause(dest),
							ErrorMsg.HOLD_DDLTIME_ON_NONEXIST_PARTITIONS
									.getMsg()));
					}
				} catch (HiveException e) {
					throw new SemanticException(e);
				}
				LOG.info("this query will not update transient_lastDdlTime!");
				ltd.setHoldDDLTime(true);
			}
			loadTableWork.add(ltd);
			if (!outputs.add(new WriteEntity(dest_part))) {
				throw new SemanticException(
						ErrorMsg.OUTPUT_SPECIFIED_MULTIPLE_TIMES
								.getMsg(dest_tab.getTableName() + "@"
										+ dest_part.getName()));
			}
			break;
		}
		case QBMetaData.DEST_LOCAL_FILE:
			isLocal = true;
			// fall through
		case QBMetaData.DEST_DFS_FILE: {
			dest_path = new Path(qbm.getDestFileForAlias(dest));
			String destStr = dest_path.toString();

			if (isLocal) {
				// for local directory - we always write to map-red intermediate
				// store and then copy to local fs
				queryTmpdir = ctx.getMRTmpFileURI();
			} else {
				// otherwise write to the file system implied by the directory
				// no copy is required. we may want to revisit this policy in
				// future

				try {
					Path qPath = FileUtils.makeQualified(dest_path, conf);
					queryTmpdir = ctx.getExternalTmpFileURI(qPath.toUri());
				} catch (Exception e) {
					throw new SemanticException(
							"Error creating temporary folder on: " + dest_path,
							e);
				}
			}
			String cols = "";
			String colTypes = "";
			ArrayList<ColumnInfo> colInfos = inputRR.getColumnInfos();

			// CTAS case: the file output format and serde are defined by the
			// create
			// table command
			// rather than taking the default value
			List<FieldSchema> field_schemas = null;
			CreateTableDesc tblDesc = querygraph.getTableDesc();
			if (tblDesc != null) {
				field_schemas = new ArrayList<FieldSchema>();
			}

			boolean first = true;
			for (ColumnInfo colInfo : colInfos) {
				String[] nm = inputRR.reverseLookup(colInfo.getInternalName());

				if (nm[1] != null) { // non-null column alias
					colInfo.setAlias(nm[1]);
				}

				if (field_schemas != null) {
					FieldSchema col = new FieldSchema();
					if (nm[1] != null) {
						col.setName(unescapeIdentifier(colInfo.getAlias())
								.toLowerCase()); // remove ``
					} else {
						col.setName(colInfo.getInternalName());
					}
					col.setType(colInfo.getType().getTypeName());
					field_schemas.add(col);
				}

				if (!first) {
					cols = cols.concat(",");
					colTypes = colTypes.concat(":");
				}

				first = false;
				cols = cols.concat(colInfo.getInternalName());

				// Replace VOID type with string when the output is a temp table
				// or
				// local files.
				// A VOID type can be generated under the query:
				//
				// select NULL from tt;
				// or
				// insert overwrite local directory "abc" select NULL from tt;
				//
				// where there is no column type to which the NULL value should
				// be
				// converted.
				//
				String tName = colInfo.getType().getTypeName();
				if (tName.equals(serdeConstants.VOID_TYPE_NAME)) {
					colTypes = colTypes.concat(serdeConstants.STRING_TYPE_NAME);
				} else {
					colTypes = colTypes.concat(tName);
				}
			}

			// update the create table descriptor with the resulting schema.
			if (tblDesc != null) {
				tblDesc.setCols(new ArrayList<FieldSchema>(field_schemas));
			}

			if (!ctx.isMRTmpFileURI(destStr)) {
				idToTableNameMap.put(String.valueOf(destTableId), destStr);
				currentTableId = destTableId;
				destTableId++;
			}

			boolean isDfsDir = (dest_type.intValue() == QBMetaData.DEST_DFS_FILE);
			loadFileWork.add(new LoadFileDesc(tblDesc, queryTmpdir, destStr,
					isDfsDir, cols, colTypes));

			if (tblDesc == null) {
				if (querygraph.getIsQuery()) {
					String fileFormat = HiveConf.getVar(conf,
						HiveConf.ConfVars.HIVEQUERYRESULTFILEFORMAT);
					table_desc = PlanUtils.getDefaultQueryOutputTableDesc(cols,
						colTypes, fileFormat);
				} else {
					table_desc = PlanUtils.getDefaultTableDesc(
						Integer.toString(Utilities.ctrlaCode), cols, colTypes,
						false);
				}
			} else {
				table_desc = PlanUtils.getTableDesc(tblDesc, cols, colTypes);
			}

			if (!outputs.add(new WriteEntity(destStr, !isDfsDir))) {
				throw new SemanticException(
						ErrorMsg.OUTPUT_SPECIFIED_MULTIPLE_TIMES
								.getMsg(destStr));
			}
			break;
		}
		default:
			throw new SemanticException("Unknown destination type: "
					+ dest_type);
		}

		input = genConversionSelectOperator(dest, querygraph, input,
			table_desc, dpCtx);
		inputRR = opParseCtx.get(input).getRowResolver();

		ArrayList<ColumnInfo> vecCol = new ArrayList<ColumnInfo>();

		try {
			StructObjectInspector rowObjectInspector = (StructObjectInspector) table_desc
					.getDeserializer().getObjectInspector();
			List<? extends StructField> fields = rowObjectInspector
					.getAllStructFieldRefs();
			for (int i = 0; i < fields.size(); i++) {
				vecCol.add(new ColumnInfo(fields.get(i).getFieldName(),
						TypeInfoUtils.getTypeInfoFromObjectInspector(fields
								.get(i).getFieldObjectInspector()), "", false));
			}
		} catch (Exception e) {
			throw new SemanticException(e.getMessage(), e);
		}

		RowSchema fsRS = new RowSchema(vecCol);

		// The output files of a FileSink can be merged if they are either not
		// being written to a table
		// or are being written to a table which is either not bucketed or
		// enforce bucketing is not set
		// and table the table is either not sorted or enforce sorting is not
		// set
		boolean canBeMerged = (dest_tab == null || !((dest_tab.getNumBuckets() > 0 && conf
				.getBoolVar(HiveConf.ConfVars.HIVEENFORCEBUCKETING)) || (dest_tab
				.getSortCols() != null && dest_tab.getSortCols().size() > 0 && conf
				.getBoolVar(HiveConf.ConfVars.HIVEENFORCESORTING))));

		FileSinkDesc fileSinkDesc = new FileSinkDesc(queryTmpdir, table_desc,
				conf.getBoolVar(HiveConf.ConfVars.COMPRESSRESULT),
				currentTableId, rsCtx.isMultiFileSpray(), canBeMerged,
				rsCtx.getNumFiles(), rsCtx.getTotalFiles(),
				rsCtx.getPartnCols(), dpCtx);

		/* Set List Bucketing context. */
		if (lbCtx != null) {
			lbCtx.processRowSkewedIndex(fsRS);
			lbCtx.calculateSkewedValueSubDirList();
		}
		fileSinkDesc.setLbCtx(lbCtx);

		// set the stats publishing/aggregating key prefix
		// the same as directory name. The directory name
		// can be changed in the optimizer but the key should not be changed
		// it should be the same as the MoveWork's sourceDir.
		fileSinkDesc.setStatsAggPrefix(fileSinkDesc.getDirName());

		if (dest_part != null) {
			try {
				String staticSpec = Warehouse.makePartPath(dest_part.getSpec());
				fileSinkDesc.setStaticSpec(staticSpec);
			} catch (MetaException e) {
				throw new SemanticException(e);
			}
		} else if (dpCtx != null) {
			fileSinkDesc.setStaticSpec(dpCtx.getSPPath());
		}

		Operator output = putOpInsertMap(
			OperatorFactory.getAndMakeChild(fileSinkDesc, fsRS, input), inputRR);

		if (ltd != null && SessionState.get() != null) {
			SessionState.get().getLineageState()
					.mapDirToFop(ltd.getSourceDir(), (FileSinkOperator) output);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Created FileSink Plan for clause: " + dest
					+ "dest_path: " + dest_path + " row schema: "
					+ inputRR.toString());
		}

		return output;
	}

	/**
	 * Generate the conversion SelectOperator that converts the columns into the
	 * types that are expected by the table_desc.
	 */
	@SuppressWarnings("rawtypes")
	Operator genConversionSelectOperator(String dest, QueryGraph querygraph,
			Operator input, TableDesc table_desc, DynamicPartitionCtx dpCtx)
			throws SemanticException {
		StructObjectInspector oi = null;
		try {
			Deserializer deserializer = table_desc.getDeserializerClass()
					.newInstance();
			deserializer.initialize(conf, table_desc.getProperties());
			oi = (StructObjectInspector) deserializer.getObjectInspector();
		} catch (Exception e) {
			throw new SemanticException(e);
		}

		// Check column number
		List<? extends StructField> tableFields = oi.getAllStructFieldRefs();
		boolean dynPart = HiveConf.getBoolVar(conf,
			HiveConf.ConfVars.DYNAMICPARTITIONING);
		ArrayList<ColumnInfo> rowFields = opParseCtx.get(input)
				.getRowResolver().getColumnInfos();
		int inColumnCnt = rowFields.size();
		int outColumnCnt = tableFields.size();
		if (dynPart && dpCtx != null) {
			outColumnCnt += dpCtx.getNumDPCols();
		}

		if (inColumnCnt != outColumnCnt) {
			String reason = "Table " + dest + " has " + outColumnCnt
					+ " columns, but query has " + inColumnCnt + " columns.";
			throw new SemanticException(
					ErrorMsg.TARGET_TABLE_COLUMN_MISMATCH.getMsg(querygraph
							.getParseInfo().getDestForClause(dest), reason));
		} else if (dynPart && dpCtx != null) {
			// create the mapping from input ExprNode to dest table DP column
			dpCtx.mapInputToDP(rowFields.subList(tableFields.size(),
				rowFields.size()));
		}

		// Check column types
		boolean converted = false;
		int columnNumber = tableFields.size();
		ArrayList<ExprNodeDesc> expressions = new ArrayList<ExprNodeDesc>(
				columnNumber);
		// MetadataTypedColumnsetSerDe does not need type conversions because it
		// does the conversion to String by itself.
		boolean isMetaDataSerDe = table_desc.getDeserializerClass().equals(
			MetadataTypedColumnsetSerDe.class);
		boolean isLazySimpleSerDe = table_desc.getDeserializerClass().equals(
			LazySimpleSerDe.class);
		if (!isMetaDataSerDe) {

			// here only deals with non-partition columns. We deal with
			// partition columns next
			for (int i = 0; i < columnNumber; i++) {
				ObjectInspector tableFieldOI = tableFields.get(i)
						.getFieldObjectInspector();
				TypeInfo tableFieldTypeInfo = TypeInfoUtils
						.getTypeInfoFromObjectInspector(tableFieldOI);
				TypeInfo rowFieldTypeInfo = rowFields.get(i).getType();
				ExprNodeDesc column = new ExprNodeColumnDesc(rowFieldTypeInfo,
						rowFields.get(i).getInternalName(), "", false,
						rowFields.get(i).isSkewedCol());
				// LazySimpleSerDe can convert any types to String type using
				// JSON-format.
				if (!tableFieldTypeInfo.equals(rowFieldTypeInfo)
						&& !(isLazySimpleSerDe
								&& tableFieldTypeInfo.getCategory().equals(
									Category.PRIMITIVE) && tableFieldTypeInfo
									.equals(TypeInfoFactory.stringTypeInfo))) {
					// need to do some conversions here
					converted = true;
					if (tableFieldTypeInfo.getCategory() != Category.PRIMITIVE) {
						// cannot convert to complex types
						column = null;
					} else {
						column = TypeCheckProcFactory.DefaultExprProcessor
								.getFuncExprNodeDesc(
									tableFieldTypeInfo.getTypeName(), column);
					}
					if (column == null) {
						String reason = "Cannot convert column " + i + " from "
								+ rowFieldTypeInfo + " to "
								+ tableFieldTypeInfo + ".";
						throw new SemanticException(
								ErrorMsg.TARGET_TABLE_COLUMN_MISMATCH.getMsg(
									querygraph.getParseInfo().getDestForClause(
										dest), reason));
					}
				}
				expressions.add(column);
			}
		}

		// deal with dynamic partition columns: convert ExprNodeDesc type to
		// String??
		if (dynPart && dpCtx != null && dpCtx.getNumDPCols() > 0) {
			// DP columns starts with tableFields.size()
			for (int i = tableFields.size(); i < rowFields.size(); ++i) {
				TypeInfo rowFieldTypeInfo = rowFields.get(i).getType();
				ExprNodeDesc column = new ExprNodeColumnDesc(rowFieldTypeInfo,
						rowFields.get(i).getInternalName(), "", false);
				expressions.add(column);
			}
			// converted = true; // [TODO]: should we check & convert type to
			// String and set it to true?
		}

		if (converted) {
			// add the select operator
			RowResolver rowResolver = new RowResolver();
			ArrayList<String> colName = new ArrayList<String>();
			for (int i = 0; i < expressions.size(); i++) {
				String name = getColumnInternalName(i);
				rowResolver.put("", name,
					new ColumnInfo(name, expressions.get(i).getTypeInfo(), "",
							false));
				colName.add(name);
			}
			Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
				new SelectDesc(expressions, colName),
				new RowSchema(rowResolver.getColumnInfos()), input),
				rowResolver);

			return output;
		} else {
			// not converted
			return input;
		}
	}

	@SuppressWarnings({ "nls", "rawtypes" })
	private Operator genBucketingSortingDest(String dest, Operator input,
			QueryGraph querygraph, TableDesc table_desc, Table dest_tab,
			SortBucketRSCtx ctx) throws SemanticException {

		// If the table is bucketed, and bucketing is enforced, do the
		// following:
		// If the number of buckets is smaller than the number of maximum
		// reducers,
		// create those many reducers.
		// If not, create a multiFileSink instead of FileSink - the
		// multiFileSink will
		// spray the data into multiple buckets. That way, we can support a very
		// large
		// number of buckets without needing a very large number of reducers.
		boolean enforceBucketing = false;
		boolean enforceSorting = false;
		ArrayList<ExprNodeDesc> partnCols = new ArrayList<ExprNodeDesc>();
		ArrayList<ExprNodeDesc> partnColsNoConvert = new ArrayList<ExprNodeDesc>();
		ArrayList<ExprNodeDesc> sortCols = new ArrayList<ExprNodeDesc>();
		ArrayList<Integer> sortOrders = new ArrayList<Integer>();
		boolean multiFileSpray = false;
		int numFiles = 1;
		int totalFiles = 1;

		if ((dest_tab.getNumBuckets() > 0)
				&& (conf.getBoolVar(HiveConf.ConfVars.HIVEENFORCEBUCKETING))) {
			enforceBucketing = true;
			partnCols = getParitionColsFromBucketCols(dest, querygraph,
				dest_tab, table_desc, input, true);
			partnColsNoConvert = getParitionColsFromBucketCols(dest,
				querygraph, dest_tab, table_desc, input, false);
		}

		if ((dest_tab.getSortCols() != null)
				&& (dest_tab.getSortCols().size() > 0)
				&& (conf.getBoolVar(HiveConf.ConfVars.HIVEENFORCESORTING))) {
			enforceSorting = true;
			sortCols = getSortCols(dest, querygraph, dest_tab, table_desc,
				input, true);
			sortOrders = getSortOrders(dest, querygraph, dest_tab, input);
			if (!enforceBucketing) {
				partnCols = sortCols;
				partnColsNoConvert = getSortCols(dest, querygraph, dest_tab,
					table_desc, input, false);
			}
		}

		if (enforceBucketing || enforceSorting) {
			int maxReducers = conf.getIntVar(HiveConf.ConfVars.MAXREDUCERS);
			if (conf.getIntVar(HiveConf.ConfVars.HADOOPNUMREDUCERS) > 0) {
				maxReducers = conf
						.getIntVar(HiveConf.ConfVars.HADOOPNUMREDUCERS);
			}
			int numBuckets = dest_tab.getNumBuckets();
			if (numBuckets > maxReducers) {
				multiFileSpray = true;
				totalFiles = numBuckets;
				if (totalFiles % maxReducers == 0) {
					numFiles = totalFiles / maxReducers;
				} else {
					// find the number of reducers such that it is a divisor of
					// totalFiles
					maxReducers = getReducersBucketing(totalFiles, maxReducers);
					numFiles = totalFiles / maxReducers;
				}
			} else {
				maxReducers = numBuckets;
			}

			input = genReduceSinkPlanForSortingBucketing(dest_tab, input,
				sortCols, sortOrders, partnCols, maxReducers);
			ctx.setMultiFileSpray(multiFileSpray);
			ctx.setNumFiles(numFiles);
			ctx.setPartnCols(partnColsNoConvert);
			ctx.setTotalFiles(totalFiles);
		}
		return input;
	}

	@SuppressWarnings({ "nls", "rawtypes", "unchecked" })
	private Operator genReduceSinkPlanForSortingBucketing(Table tab,
			Operator input, ArrayList<ExprNodeDesc> sortCols,
			List<Integer> sortOrders, ArrayList<ExprNodeDesc> partitionCols,
			int numReducers) throws SemanticException {
		RowResolver inputRR = opParseCtx.get(input).getRowResolver();

		// For the generation of the values expression just get the inputs
		// signature and generate field expressions for those
		Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
		ArrayList<ExprNodeDesc> valueCols = new ArrayList<ExprNodeDesc>();
		for (ColumnInfo colInfo : inputRR.getColumnInfos()) {
			valueCols.add(new ExprNodeColumnDesc(colInfo.getType(), colInfo
					.getInternalName(), colInfo.getTabAlias(), colInfo
					.getIsVirtualCol()));
			colExprMap.put(colInfo.getInternalName(),
				valueCols.get(valueCols.size() - 1));
		}

		ArrayList<String> outputColumns = new ArrayList<String>();
		for (int i = 0; i < valueCols.size(); i++) {
			outputColumns.add(getColumnInternalName(i));
		}

		StringBuilder order = new StringBuilder();
		for (int sortOrder : sortOrders) {
			order.append(sortOrder == BaseSemanticAnalyzer.HIVE_COLUMN_ORDER_ASC ? '+'
					: '-');
		}

		Operator interim = putOpInsertMap(OperatorFactory.getAndMakeChild(
			PlanUtils.getReduceSinkDesc(sortCols, valueCols, outputColumns,
				false, -1, partitionCols, order.toString(), numReducers),
			new RowSchema(inputRR.getColumnInfos()), input), inputRR);
		interim.setColumnExprMap(colExprMap);

		// Add the extract operator to get the value fields
		RowResolver out_rwsch = new RowResolver();
		RowResolver interim_rwsch = inputRR;
		Integer pos = Integer.valueOf(0);
		for (ColumnInfo colInfo : interim_rwsch.getColumnInfos()) {
			String[] info = interim_rwsch.reverseLookup(colInfo
					.getInternalName());
			out_rwsch.put(info[0], info[1], new ColumnInfo(
					getColumnInternalName(pos), colInfo.getType(), info[0],
					colInfo.getIsVirtualCol(), colInfo.isHiddenVirtualCol()));
			pos = Integer.valueOf(pos.intValue() + 1);
		}

		Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
			new ExtractDesc(new ExprNodeColumnDesc(
					TypeInfoFactory.stringTypeInfo, Utilities.ReduceField.VALUE
							.toString(), "", false)),
			new RowSchema(out_rwsch.getColumnInfos()), interim), out_rwsch);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Created ReduceSink Plan for table: "
					+ tab.getTableName() + " row schema: "
					+ out_rwsch.toString());
		}
		return output;

	}

	private int getReducersBucketing(int totalFiles, int maxReducers) {
		int numFiles = (int) Math.ceil((double) totalFiles
				/ (double) maxReducers);
		while (true) {
			if (totalFiles % numFiles == 0) {
				return totalFiles / numFiles;
			}
			numFiles++;
		}
	}

	@SuppressWarnings("rawtypes")
	private ArrayList<ExprNodeDesc> getSortCols(String dest,
			QueryGraph querygraph, Table tab, TableDesc table_desc,
			Operator input, boolean convert) throws SemanticException {
		// RowResolver inputRR = opParseCtx.get(input).getRowResolver();
		List<Order> tabSortCols = tab.getSortCols();
		List<FieldSchema> tabCols = tab.getCols();

		// Partition by the bucketing column
		List<Integer> posns = new ArrayList<Integer>();
		for (Order sortCol : tabSortCols) {
			int pos = 0;
			for (FieldSchema tabCol : tabCols) {
				if (sortCol.getCol().equals(tabCol.getName())) {
					// ColumnInfo colInfo = inputRR.getColumnInfos().get(pos);
					posns.add(pos);
					break;
				}
				pos++;
			}
		}

		return genConvertCol(dest, querygraph, tab, table_desc, input, posns,
			convert);
	}

	@SuppressWarnings("rawtypes")
	private ArrayList<ExprNodeDesc> genConvertCol(String dest,
			QueryGraph querygraph, Table tab, TableDesc table_desc,
			Operator input, List<Integer> posns, boolean convert)
			throws SemanticException {
		StructObjectInspector oi = null;
		try {
			Deserializer deserializer = table_desc.getDeserializerClass()
					.newInstance();
			deserializer.initialize(conf, table_desc.getProperties());
			oi = (StructObjectInspector) deserializer.getObjectInspector();
		} catch (Exception e) {
			throw new SemanticException(e);
		}

		List<? extends StructField> tableFields = oi.getAllStructFieldRefs();
		ArrayList<ColumnInfo> rowFields = opParseCtx.get(input)
				.getRowResolver().getColumnInfos();

		// Check column type
		int columnNumber = posns.size();
		ArrayList<ExprNodeDesc> expressions = new ArrayList<ExprNodeDesc>(
				columnNumber);
		for (Integer posn : posns) {
			ObjectInspector tableFieldOI = tableFields.get(posn)
					.getFieldObjectInspector();
			TypeInfo tableFieldTypeInfo = TypeInfoUtils
					.getTypeInfoFromObjectInspector(tableFieldOI);
			TypeInfo rowFieldTypeInfo = rowFields.get(posn).getType();
			ExprNodeDesc column = new ExprNodeColumnDesc(rowFieldTypeInfo,
					rowFields.get(posn).getInternalName(), rowFields.get(posn)
							.getTabAlias(), rowFields.get(posn)
							.getIsVirtualCol());

			if (convert && !tableFieldTypeInfo.equals(rowFieldTypeInfo)) {
				// need to do some conversions here
				if (tableFieldTypeInfo.getCategory() != Category.PRIMITIVE) {
					// cannot convert to complex types
					column = null;
				} else {
					column = TypeCheckProcFactory.DefaultExprProcessor
							.getFuncExprNodeDesc(
								tableFieldTypeInfo.getTypeName(), column);
				}
				if (column == null) {
					String reason = "Cannot convert column " + posn + " from "
							+ rowFieldTypeInfo + " to " + tableFieldTypeInfo
							+ ".";
					throw new SemanticException(
							ErrorMsg.TARGET_TABLE_COLUMN_MISMATCH.getMsg(
								querygraph.getParseInfo()
										.getDestForClause(dest), reason));
				}
			}
			expressions.add(column);
		}

		return expressions;
	}

	@SuppressWarnings("rawtypes")
	private ArrayList<Integer> getSortOrders(String dest,
			QueryGraph querygraph, Table tab, Operator input)
			throws SemanticException {
		// RowResolver inputRR = opParseCtx.get(input).getRowResolver();
		List<Order> tabSortCols = tab.getSortCols();
		List<FieldSchema> tabCols = tab.getCols();

		ArrayList<Integer> orders = new ArrayList<Integer>();
		for (Order sortCol : tabSortCols) {
			for (FieldSchema tabCol : tabCols) {
				if (sortCol.getCol().equals(tabCol.getName())) {
					orders.add(sortCol.getOrder());
					break;
				}
			}
		}
		return orders;
	}

	@SuppressWarnings("rawtypes")
	private ArrayList<ExprNodeDesc> getParitionColsFromBucketCols(String dest,
			QueryGraph querygraph, Table tab, TableDesc table_desc,
			Operator input, boolean convert) throws SemanticException {
		List<String> tabBucketCols = tab.getBucketCols();
		List<FieldSchema> tabCols = tab.getCols();

		// Partition by the bucketing column
		List<Integer> posns = new ArrayList<Integer>();

		for (String bucketCol : tabBucketCols) {
			int pos = 0;
			for (FieldSchema tabCol : tabCols) {
				if (bucketCol.equals(tabCol.getName())) {
					posns.add(pos);
					break;
				}
				pos++;
			}
		}

		return genConvertCol(dest, querygraph, tab, table_desc, input, posns,
			convert);
	}

	@SuppressWarnings({ "nls", "rawtypes" })
	private Operator genConversionOps(String dest, QueryGraph querygraph,
			Operator input) throws SemanticException {

		Integer dest_type = querygraph.getMetaData().getDestTypeForAlias(dest);
		switch (dest_type.intValue()) {
		case QBMetaData.DEST_TABLE: {
			querygraph.getMetaData().getDestTableForAlias(dest);
			break;
		}
		case QBMetaData.DEST_PARTITION: {
			querygraph.getMetaData().getDestPartitionForAlias(dest).getTable();
			break;
		}
		default: {
			return input;
		}
		}

		return input;
	}

	private Operator<?> genSelectPlan(String dest, QueryGraph querygraph,
			Operator<?> input) throws SemanticException {
		ASTNode selExprList = querygraph.getParseInfo().getSelForClause(dest);

		Operator<?> op = genSelectPlan(selExprList, querygraph, input);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Created Select Plan for clause: " + dest);
		}

		return op;
	}

	@SuppressWarnings({ "nls", "unchecked", "rawtypes", "null" })
	private Operator<?> genSelectPlan(ASTNode selExprList,
			QueryGraph querygraph, Operator<?> input) throws SemanticException {

		if (LOG.isDebugEnabled()) {
			LOG.debug("tree: " + selExprList.toStringTree());
		}

		ArrayList<ExprNodeDesc> col_list = new ArrayList<ExprNodeDesc>();
		RowResolver out_rwsch = new RowResolver();
		ASTNode trfm = null;
		Integer pos = Integer.valueOf(0);
		RowResolver inputRR = opParseCtx.get(input).getRowResolver();
		// SELECT * or SELECT TRANSFORM(*)
		boolean selectStar = false;
		int posn = 0;
		boolean hintPresent = (selExprList.getChild(0).getType() == HiveParser.TOK_HINTLIST);
		if (hintPresent) {
			posn++;
		}

		boolean isInTransform = (selExprList.getChild(posn).getChild(0)
				.getType() == HiveParser.TOK_TRANSFORM);
		if (isInTransform) {
			// queryProperties.setUsesScript(true);
			// globalLimitCtx.setHasTransformOrUDTF(true);
			// trfm = (ASTNode) selExprList.getChild(posn).getChild(0);
		}

		// Detect queries of the form SELECT udtf(col) AS ...
		// by looking for a function as the first child, and then checking to
		// see
		// if the function is a Generic UDTF. It's not as clean as TRANSFORM due
		// to
		// the lack of a special token.
		boolean isUDTF = false;
		String udtfTableAlias = null;
		ArrayList<String> udtfColAliases = new ArrayList<String>();
		ASTNode udtfExpr = (ASTNode) selExprList.getChild(posn).getChild(0);
		GenericUDTF genericUDTF = null;

		int udtfExprType = udtfExpr.getType();
		if (udtfExprType == HiveParser.TOK_FUNCTION
				|| udtfExprType == HiveParser.TOK_FUNCTIONSTAR) {
			// String funcName = TypeCheckProcFactory.DefaultExprProcessor
			// .getFunctionText(udtfExpr, true);
			// FunctionInfo fi = FunctionRegistry.getFunctionInfo(funcName);
			// if (fi != null) {
			// genericUDTF = fi.getGenericUDTF();
			// }
			// isUDTF = (genericUDTF != null);
			// if (isUDTF) {
			// globalLimitCtx.setHasTransformOrUDTF(true);
			// }
			// if (isUDTF && !fi.isNative()) {
			// unparseTranslator.addIdentifierTranslation((ASTNode) udtfExpr
			// .getChild(0));
			// }
		}

		// if (isUDTF) {
		// // Only support a single expression when it's a UDTF
		// if (selExprList.getChildCount() > 1) {
		// throw new SemanticException(generateErrorMessage(
		// (ASTNode) selExprList.getChild(1),
		// ErrorMsg.UDTF_MULTIPLE_EXPR.getMsg()));
		// }
		// // Require an AS for UDTFs for column aliases
		// ASTNode selExpr = (ASTNode) selExprList.getChild(posn);
		// if (selExpr.getChildCount() < 2) {
		// throw new SemanticException(generateErrorMessage(udtfExpr,
		// ErrorMsg.UDTF_REQUIRE_AS.getMsg()));
		// }
		// // Get the column / table aliases from the expression. Start from 1
		// // as 0 is the TOK_FUNCTION
		// for (int i = 1; i < selExpr.getChildCount(); i++) {
		// ASTNode selExprChild = (ASTNode) selExpr.getChild(i);
		// switch (selExprChild.getType()) {
		// case HiveParser.Identifier:
		// udtfColAliases.add(unescapeIdentifier(selExprChild
		// .getText()));
		// unparseTranslator.addIdentifierTranslation(selExprChild);
		// break;
		// case HiveParser.TOK_TABALIAS:
		// assert (selExprChild.getChildCount() == 1);
		// udtfTableAlias = unescapeIdentifier(selExprChild
		// .getChild(0).getText());
		// querygraph.addAlias(udtfTableAlias);
		// unparseTranslator
		// .addIdentifierTranslation((ASTNode) selExprChild
		// .getChild(0));
		// break;
		// default:
		// assert (false);
		// }
		// }
		// if (LOG.isDebugEnabled()) {
		// LOG.debug("UDTF table alias is " + udtfTableAlias);
		// LOG.debug("UDTF col aliases are " + udtfColAliases);
		// }
		// }

		// The list of expressions after SELECT or SELECT TRANSFORM.
		ASTNode exprList;
		if (isInTransform) {
			exprList = (ASTNode) trfm.getChild(0);
		} else if (isUDTF) {
			exprList = udtfExpr;
		} else {
			exprList = selExprList;
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("genSelectPlan: input = " + inputRR.toString());
		}

		// For UDTF's, skip the function name to get the expressions
		int startPosn = 0;
		if (isInTransform) {
			startPosn = 0;
		}

		// Iterate over all expression (either after SELECT, or in SELECT
		// TRANSFORM)
		// for (int i = startPosn; i < exprList.getChildCount(); ++i) {
		for (int i = startPosn; i < querygraph.getProjectionsNums(); ++i) {

			// child can be EXPR AS ALIAS, or EXPR.
			ASTNode child = (ASTNode) exprList.getChild(i);
			boolean hasAsClause = false;

			// EXPR AS (ALIAS,...) parses, but is only allowed for UDTF's
			// This check is not needed and invalid when there is a transform
			// b/c the
			// AST's are slightly different.
			// if (!isInTransform && !isUDTF && child.getChildCount() > 2) {
			// throw new SemanticException(generateErrorMessage(
			// (ASTNode) child.getChild(2),
			// ErrorMsg.INVALID_AS.getMsg()));
			// }

			String tabAlias;
			String colAlias;
			String[] node = new String[2];

			if (isInTransform || isUDTF) {
				tabAlias = null;
				colAlias = autogenColAliasPrfxLbl + i;
			} else {
				// String[] colRef = getColAlias(child, autogenColAliasPrfxLbl,
				// inputRR, autogenColAliasPrfxIncludeFuncName, i);
				// tabAlias = colRef[0];
				// colAlias = colRef[1];
				colAlias = querygraph.getProjectionToAlias().get(i);
				tabAlias = querygraph.getProjectionToTable().get(i);
				node[0] = tabAlias;
				node[1] = colAlias;

				if (hasAsClause) {
					unparseTranslator.addIdentifierTranslation((ASTNode) child
							.getChild(1));
				}

			}

			boolean subQuery = querygraph.getParseInfo().getIsSubQ();
			// if (expr.getType() == HiveParser.TOK_ALLCOLREF) {
			// pos = genColListRegex(".*", expr.getChildCount() == 0 ? null
			// : getUnescapedName((ASTNode) expr.getChild(0))
			// .toLowerCase(), expr, col_list, inputRR, pos,
			// out_rwsch, querygraph.getAliases(), subQuery);
			// selectStar = true;
			// } else if (expr.getType() == HiveParser.TOK_TABLE_OR_COL
			// && !hasAsClause && !inputRR.getIsExprResolver()
			// && isRegex(unescapeIdentifier(expr.getChild(0).getText()))) {
			// // In case the expression is a regex COL.
			// // This can only happen without AS clause
			// // We don't allow this for ExprResolver - the Group By case
			// pos = genColListRegex(unescapeIdentifier(expr.getChild(0)
			// .getText()), null, expr, col_list, inputRR, pos,
			// out_rwsch, querygraph.getAliases(), subQuery);
			// } else if (expr.getType() == HiveParser.DOT
			// && expr.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL
			// && inputRR.hasTableAlias(unescapeIdentifier(expr
			// .getChild(0).getChild(0).getText().toLowerCase()))
			// && !hasAsClause && !inputRR.getIsExprResolver()
			// && isRegex(unescapeIdentifier(expr.getChild(1).getText()))) {
			// // In case the expression is TABLE.COL (col can be regex).
			// // This can only happen without AS clause
			// // We don't allow this for ExprResolver - the Group By case
			// pos = genColListRegex(unescapeIdentifier(expr.getChild(1)
			// .getText()), unescapeIdentifier(expr.getChild(0)
			// .getChild(0).getText().toLowerCase()), expr, col_list,
			// inputRR, pos, out_rwsch, querygraph.getAliases(),
			// subQuery);
			// } else {
			// Case when this is an expression
			TypeCheckCtx tcCtx = new TypeCheckCtx(inputRR);
			// We allow stateful functions in the SELECT list (but nowhere
			// else)
			tcCtx.setAllowStatefulFunctions(true);
			ExprNodeDesc exp = genNodeDesc(node, inputRR, tcCtx);
			String recommended = recommendName(exp, colAlias);
			if (recommended != null && out_rwsch.get(null, recommended) == null) {
				colAlias = recommended;
			}
			col_list.add(exp);
			if (subQuery) {
				out_rwsch.checkColumn(tabAlias, colAlias);
			}

			ColumnInfo colInfo = new ColumnInfo(getColumnInternalName(pos),
					exp.getWritableObjectInspector(), tabAlias, false);
			colInfo.setSkewedCol((exp instanceof ExprNodeColumnDesc) ? ((ExprNodeColumnDesc) exp)
					.isSkewedCol() : false);
			out_rwsch.put(tabAlias, colAlias, colInfo);

			pos = Integer.valueOf(pos.intValue() + 1);
			// }
		}
		selectStar = selectStar && exprList.getChildCount() == posn + 1;

		ArrayList<String> columnNames = new ArrayList<String>();
		Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
		for (int i = 0; i < col_list.size(); i++) {
			// Replace NULL with CAST(NULL AS STRING)
			if (col_list.get(i) instanceof ExprNodeNullDesc) {
				col_list.set(i, new ExprNodeConstantDesc(
						TypeInfoFactory.stringTypeInfo, null));
			}
			String outputCol = getColumnInternalName(i);
			colExprMap.put(outputCol, col_list.get(i));
			columnNames.add(outputCol);
		}

		Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
			new SelectDesc(col_list, columnNames, selectStar), new RowSchema(
					out_rwsch.getColumnInfos()), input), out_rwsch);

		output.setColumnExprMap(colExprMap);
		if (isInTransform) {
			output = genScriptPlan(trfm, querygraph, output);
		}

		if (isUDTF) {
			output = genUDTFPlan(genericUDTF, udtfTableAlias, udtfColAliases,
				querygraph, output);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("Created Select Plan row schema: " + out_rwsch.toString());
		}
		return output;
	}

	@SuppressWarnings("rawtypes")
	private Operator genUDTFPlan(GenericUDTF genericUDTF,
			String outputTableAlias, ArrayList<String> colAliases,
			QueryGraph querygraph, Operator input) throws SemanticException {

		// No GROUP BY / DISTRIBUTE BY / SORT BY / CLUSTER BY
		QBParseInfo qbp = querygraph.getParseInfo();
		if (!qbp.getDestToGroupBy().isEmpty()) {
			throw new SemanticException(ErrorMsg.UDTF_NO_GROUP_BY.getMsg());
		}
		if (!qbp.getDestToDistributeBy().isEmpty()) {
			throw new SemanticException(ErrorMsg.UDTF_NO_DISTRIBUTE_BY.getMsg());
		}
		if (!qbp.getDestToSortBy().isEmpty()) {
			throw new SemanticException(ErrorMsg.UDTF_NO_SORT_BY.getMsg());
		}
		if (!qbp.getDestToClusterBy().isEmpty()) {
			throw new SemanticException(ErrorMsg.UDTF_NO_CLUSTER_BY.getMsg());
		}
		if (!qbp.getAliasToLateralViews().isEmpty()) {
			throw new SemanticException(ErrorMsg.UDTF_LATERAL_VIEW.getMsg());
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Table alias: " + outputTableAlias + " Col aliases: "
					+ colAliases);
		}

		// Use the RowResolver from the input operator to generate a input
		// ObjectInspector that can be used to initialize the UDTF. Then, the

		// resulting output object inspector can be used to make the RowResolver
		// for the UDTF operator
		RowResolver selectRR = opParseCtx.get(input).getRowResolver();
		ArrayList<ColumnInfo> inputCols = selectRR.getColumnInfos();

		// Create the object inspector for the input columns and initialize the
		// UDTF
		ArrayList<String> colNames = new ArrayList<String>();
		ObjectInspector[] colOIs = new ObjectInspector[inputCols.size()];
		for (int i = 0; i < inputCols.size(); i++) {
			colNames.add(inputCols.get(i).getInternalName());
			colOIs[i] = inputCols.get(i).getObjectInspector();
		}
		StructObjectInspector outputOI = genericUDTF.initialize(colOIs);

		// Make sure that the number of column aliases in the AS clause matches
		// the number of columns output by the UDTF
		int numUdtfCols = outputOI.getAllStructFieldRefs().size();
		int numSuppliedAliases = colAliases.size();
		if (numUdtfCols != numSuppliedAliases) {
			throw new SemanticException(
					ErrorMsg.UDTF_ALIAS_MISMATCH.getMsg("expected "
							+ numUdtfCols + " aliases " + "but got "
							+ numSuppliedAliases));
		}

		// Generate the output column info's / row resolver using internal
		// names.
		ArrayList<ColumnInfo> udtfCols = new ArrayList<ColumnInfo>();

		Iterator<String> colAliasesIter = colAliases.iterator();
		for (StructField sf : outputOI.getAllStructFieldRefs()) {

			String colAlias = colAliasesIter.next();
			assert (colAlias != null);

			// Since the UDTF operator feeds into a LVJ operator that will
			// rename
			// all the internal names, we can just use field name from the
			// UDTF's OI
			// as the internal name
			ColumnInfo col = new ColumnInfo(sf.getFieldName(),
					TypeInfoUtils.getTypeInfoFromObjectInspector(sf
							.getFieldObjectInspector()), outputTableAlias,
					false);
			udtfCols.add(col);
		}

		// Create the row resolver for this operator from the output columns
		RowResolver out_rwsch = new RowResolver();
		for (int i = 0; i < udtfCols.size(); i++) {
			out_rwsch.put(outputTableAlias, colAliases.get(i), udtfCols.get(i));
		}

		// Add the UDTFOperator to the operator DAG
		Operator<?> udtf = putOpInsertMap(OperatorFactory.getAndMakeChild(
			new UDTFDesc(genericUDTF),
			new RowSchema(out_rwsch.getColumnInfos()), input), out_rwsch);
		return udtf;
	}

	@SuppressWarnings({ "nls", "unchecked", "rawtypes" })
	private Operator genScriptPlan(ASTNode trfm, QueryGraph querygraph,
			Operator input) throws SemanticException {
		// If there is no "AS" clause, the output schema will be "key,value"
		ArrayList<ColumnInfo> outputCols = new ArrayList<ColumnInfo>();
		int inputSerDeNum = 1, inputRecordWriterNum = 2;
		int outputSerDeNum = 4, outputRecordReaderNum = 5;
		int outputColsNum = 6;
		boolean outputColNames = false, outputColSchemas = false;
		int execPos = 3;
		boolean defaultOutputCols = false;

		// Go over all the children
		if (trfm.getChildCount() > outputColsNum) {
			ASTNode outCols = (ASTNode) trfm.getChild(outputColsNum);
			if (outCols.getType() == HiveParser.TOK_ALIASLIST) {
				outputColNames = true;
			} else if (outCols.getType() == HiveParser.TOK_TABCOLLIST) {
				outputColSchemas = true;
			}
		}

		// If column type is not specified, use a string
		if (!outputColNames && !outputColSchemas) {
			String intName = getColumnInternalName(0);
			ColumnInfo colInfo = new ColumnInfo(intName,
					TypeInfoFactory.stringTypeInfo, null, false);
			colInfo.setAlias("key");
			outputCols.add(colInfo);
			intName = getColumnInternalName(1);
			colInfo = new ColumnInfo(intName, TypeInfoFactory.stringTypeInfo,
					null, false);
			colInfo.setAlias("value");
			outputCols.add(colInfo);
			defaultOutputCols = true;
		} else {
			ASTNode collist = (ASTNode) trfm.getChild(outputColsNum);
			int ccount = collist.getChildCount();

			Set<String> colAliasNamesDuplicateCheck = new HashSet<String>();
			if (outputColNames) {
				for (int i = 0; i < ccount; ++i) {
					String colAlias = unescapeIdentifier(((ASTNode) collist
							.getChild(i)).getText());
					failIfColAliasExists(colAliasNamesDuplicateCheck, colAlias);
					String intName = getColumnInternalName(i);
					ColumnInfo colInfo = new ColumnInfo(intName,
							TypeInfoFactory.stringTypeInfo, null, false);
					colInfo.setAlias(colAlias);
					outputCols.add(colInfo);
				}
			} else {
				for (int i = 0; i < ccount; ++i) {
					ASTNode child = (ASTNode) collist.getChild(i);
					assert child.getType() == HiveParser.TOK_TABCOL;
					String colAlias = unescapeIdentifier(((ASTNode) child
							.getChild(0)).getText());
					failIfColAliasExists(colAliasNamesDuplicateCheck, colAlias);
					String intName = getColumnInternalName(i);
					ColumnInfo colInfo = new ColumnInfo(
							intName,
							TypeInfoUtils
									.getTypeInfoFromTypeString(getTypeStringFromAST((ASTNode) child
											.getChild(1))), null, false);
					colInfo.setAlias(colAlias);
					outputCols.add(colInfo);
				}
			}
		}

		RowResolver out_rwsch = new RowResolver();
		StringBuilder columns = new StringBuilder();
		StringBuilder columnTypes = new StringBuilder();

		for (int i = 0; i < outputCols.size(); ++i) {
			if (i != 0) {
				columns.append(",");
				columnTypes.append(",");
			}

			columns.append(outputCols.get(i).getInternalName());
			columnTypes.append(outputCols.get(i).getType().getTypeName());

			out_rwsch.put(querygraph.getParseInfo().getAlias(),
				outputCols.get(i).getAlias(), outputCols.get(i));
		}

		StringBuilder inpColumns = new StringBuilder();
		StringBuilder inpColumnTypes = new StringBuilder();
		ArrayList<ColumnInfo> inputSchema = opParseCtx.get(input)
				.getRowResolver().getColumnInfos();
		for (int i = 0; i < inputSchema.size(); ++i) {
			if (i != 0) {
				inpColumns.append(",");
				inpColumnTypes.append(",");
			}

			inpColumns.append(inputSchema.get(i).getInternalName());
			inpColumnTypes.append(inputSchema.get(i).getType().getTypeName());
		}

		TableDesc outInfo;
		TableDesc errInfo;
		TableDesc inInfo;
		String defaultSerdeName = conf
				.getVar(HiveConf.ConfVars.HIVESCRIPTSERDE);
		Class<? extends Deserializer> serde;

		try {
			serde = (Class<? extends Deserializer>) Class.forName(
				defaultSerdeName, true, JavaUtils.getClassLoader());
		} catch (ClassNotFoundException e) {
			throw new SemanticException(e);
		}

		int fieldSeparator = Utilities.tabCode;
		if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVESCRIPTESCAPE)) {
			fieldSeparator = Utilities.ctrlaCode;
		}

		// Input and Output Serdes
		if (trfm.getChild(inputSerDeNum).getChildCount() > 0) {
			inInfo = getTableDescFromSerDe(
				(ASTNode) (((ASTNode) trfm.getChild(inputSerDeNum)))
						.getChild(0),
				inpColumns.toString(), inpColumnTypes.toString(), false);
		} else {
			inInfo = PlanUtils.getTableDesc(serde,
				Integer.toString(fieldSeparator), inpColumns.toString(),
				inpColumnTypes.toString(), false, true);
		}

		if (trfm.getChild(outputSerDeNum).getChildCount() > 0) {
			outInfo = getTableDescFromSerDe(
				(ASTNode) (((ASTNode) trfm.getChild(outputSerDeNum)))
						.getChild(0),
				columns.toString(), columnTypes.toString(), false);
			// This is for backward compatibility. If the user did not specify
			// the
			// output column list, we assume that there are 2 columns: key and
			// value.
			// However, if the script outputs: col1, col2, col3 seperated by
			// TAB, the
			// requirement is: key is col and value is (col2 TAB col3)
		} else {
			outInfo = PlanUtils.getTableDesc(serde,
				Integer.toString(fieldSeparator), columns.toString(),
				columnTypes.toString(), defaultOutputCols);
		}

		// Error stream always uses the default serde with a single column
		errInfo = PlanUtils.getTableDesc(serde,
			Integer.toString(Utilities.tabCode), "KEY");

		// Output record readers
		Class<? extends RecordReader> outRecordReader = getRecordReader((ASTNode) trfm
				.getChild(outputRecordReaderNum));
		Class<? extends RecordWriter> inRecordWriter = getRecordWriter((ASTNode) trfm
				.getChild(inputRecordWriterNum));
		Class<? extends RecordReader> errRecordReader = getDefaultRecordReader();

		Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
			new ScriptDesc(fetchFilesNotInLocalFilesystem(stripQuotes(trfm
					.getChild(execPos).getText())), inInfo, inRecordWriter,
					outInfo, outRecordReader, errRecordReader, errInfo),
			new RowSchema(out_rwsch.getColumnInfos()), input), out_rwsch);

		return output;
	}

	private String fetchFilesNotInLocalFilesystem(String cmd) {
		SessionState ss = SessionState.get();
		String progName = getScriptProgName(cmd);

		if (SessionState.canDownloadResource(progName)) {
			String filePath = ss
					.add_resource(ResourceType.FILE, progName, true);
			if (filePath == null) {
				throw new RuntimeException("Could not download the resource: "
						+ progName);
			}
			Path p = new Path(filePath);
			String fileName = p.getName();
			String scriptArgs = getScriptArgs(cmd);
			String finalCmd = fileName + scriptArgs;
			return finalCmd;
		}

		return cmd;
	}

	private String getScriptProgName(String cmd) {
		int end = cmd.indexOf(" ");
		return (end == -1) ? cmd : cmd.substring(0, end);
	}

	private String getScriptArgs(String cmd) {
		int end = cmd.indexOf(" ");
		return (end == -1) ? "" : cmd.substring(end, cmd.length());
	}

	@SuppressWarnings("unchecked")
	private Class<? extends RecordReader> getRecordReader(ASTNode node)
			throws SemanticException {
		String name;

		if (node.getChildCount() == 0) {
			name = conf.getVar(HiveConf.ConfVars.HIVESCRIPTRECORDREADER);
		} else {
			name = unescapeSQLString(node.getChild(0).getText());
		}

		try {
			return (Class<? extends RecordReader>) Class.forName(name, true,
				JavaUtils.getClassLoader());
		} catch (ClassNotFoundException e) {
			throw new SemanticException(e);
		}
	}

	@SuppressWarnings("unchecked")
	private Class<? extends RecordReader> getDefaultRecordReader()
			throws SemanticException {
		String name;

		name = conf.getVar(HiveConf.ConfVars.HIVESCRIPTRECORDREADER);

		try {
			return (Class<? extends RecordReader>) Class.forName(name, true,
				JavaUtils.getClassLoader());
		} catch (ClassNotFoundException e) {
			throw new SemanticException(e);
		}
	}

	@SuppressWarnings("unchecked")
	private Class<? extends RecordWriter> getRecordWriter(ASTNode node)
			throws SemanticException {
		String name;

		if (node.getChildCount() == 0) {
			name = conf.getVar(HiveConf.ConfVars.HIVESCRIPTRECORDWRITER);
		} else {
			name = unescapeSQLString(node.getChild(0).getText());
		}

		try {
			return (Class<? extends RecordWriter>) Class.forName(name, true,
				JavaUtils.getClassLoader());
		} catch (ClassNotFoundException e) {
			throw new SemanticException(e);
		}
	}

	@SuppressWarnings("unchecked")
	private TableDesc getTableDescFromSerDe(ASTNode child, String cols,
			String colTypes, boolean defaultCols) throws SemanticException {
		if (child.getType() == HiveParser.TOK_SERDENAME) {
			String serdeName = unescapeSQLString(child.getChild(0).getText());
			Class<? extends Deserializer> serdeClass = null;

			try {
				serdeClass = (Class<? extends Deserializer>) Class.forName(
					serdeName, true, JavaUtils.getClassLoader());
			} catch (ClassNotFoundException e) {
				throw new SemanticException(e);
			}

			TableDesc tblDesc = PlanUtils.getTableDesc(serdeClass,
				Integer.toString(Utilities.tabCode), cols, colTypes,
				defaultCols);
			// copy all the properties
			if (child.getChildCount() == 2) {
				ASTNode prop = (ASTNode) ((ASTNode) child.getChild(1))
						.getChild(0);
				for (int propChild = 0; propChild < prop.getChildCount(); propChild++) {
					String key = unescapeSQLString(prop.getChild(propChild)
							.getChild(0).getText());
					String value = unescapeSQLString(prop.getChild(propChild)
							.getChild(1).getText());
					tblDesc.getProperties().setProperty(key, value);
				}
			}
			return tblDesc;
		} else if (child.getType() == HiveParser.TOK_SERDEPROPS) {
			TableDesc tblDesc = PlanUtils.getDefaultTableDesc(
				Integer.toString(Utilities.ctrlaCode), cols, colTypes,
				defaultCols);
			int numChildRowFormat = child.getChildCount();
			for (int numC = 0; numC < numChildRowFormat; numC++) {
				ASTNode rowChild = (ASTNode) child.getChild(numC);
				switch (rowChild.getToken().getType()) {
				case HiveParser.TOK_TABLEROWFORMATFIELD:
					String fieldDelim = unescapeSQLString(rowChild.getChild(0)
							.getText());
					tblDesc.getProperties().setProperty(
						serdeConstants.FIELD_DELIM, fieldDelim);
					tblDesc.getProperties().setProperty(
						serdeConstants.SERIALIZATION_FORMAT, fieldDelim);

					if (rowChild.getChildCount() >= 2) {
						String fieldEscape = unescapeSQLString(rowChild
								.getChild(1).getText());
						tblDesc.getProperties().setProperty(
							serdeConstants.ESCAPE_CHAR, fieldEscape);
					}
					break;
				case HiveParser.TOK_TABLEROWFORMATCOLLITEMS:
					tblDesc.getProperties().setProperty(
						serdeConstants.COLLECTION_DELIM,
						unescapeSQLString(rowChild.getChild(0).getText()));
					break;
				case HiveParser.TOK_TABLEROWFORMATMAPKEYS:
					tblDesc.getProperties().setProperty(
						serdeConstants.MAPKEY_DELIM,
						unescapeSQLString(rowChild.getChild(0).getText()));
					break;
				case HiveParser.TOK_TABLEROWFORMATLINES:
					String lineDelim = unescapeSQLString(rowChild.getChild(0)
							.getText());
					tblDesc.getProperties().setProperty(
						serdeConstants.LINE_DELIM, lineDelim);
					if (!lineDelim.equals("\n") && !lineDelim.equals("10")) {
						throw new SemanticException(generateErrorMessage(
							rowChild,
							ErrorMsg.LINES_TERMINATED_BY_NON_NEWLINE.getMsg()));
					}
					break;
				default:
					assert false;
				}
			}

			return tblDesc;
		}

		// should never come here
		return null;
	}

	private void failIfColAliasExists(Set<String> nameSet, String name)
			throws SemanticException {
		if (nameSet.contains(name)) {
			throw new SemanticException(
					ErrorMsg.COLUMN_ALIAS_ALREADY_EXISTS.getMsg(name));
		}
		nameSet.add(name);
	}

	private String recommendName(ExprNodeDesc exp, String colAlias) {
		if (!colAlias.startsWith(autogenColAliasPrfxLbl)) {
			return null;
		}
		String column = ExprNodeDescUtils.recommendInputName(exp);
		if (column != null && !column.startsWith(autogenColAliasPrfxLbl)) {
			return column;
		}
		return null;
	}

	public static String getColumnInternalName(int pos) {
		return HiveConf.getColumnInternalName(pos);
	}

	/**
	 * This function is a wrapper of parseInfo.getGroupByForClause which
	 * automatically translates SELECT DISTINCT a,b,c to SELECT a,b,c GROUP BY
	 * a,b,c.
	 */
	static List<ASTNode> getGroupByForClause(QBParseInfo parseInfo, String dest) {
		if (parseInfo.getSelForClause(dest).getToken().getType() == HiveParser.TOK_SELECTDI) {
			ASTNode selectExprs = parseInfo.getSelForClause(dest);
			List<ASTNode> result = new ArrayList<ASTNode>(
					selectExprs == null ? 0 : selectExprs.getChildCount());
			if (selectExprs != null) {
				for (int i = 0; i < selectExprs.getChildCount(); ++i) {
					if (((ASTNode) selectExprs.getChild(i)).getToken()
							.getType() == HiveParser.TOK_HINTLIST) {
						continue;
					}
					// table.column AS alias
					ASTNode grpbyExpr = (ASTNode) selectExprs.getChild(i)
							.getChild(0);
					result.add(grpbyExpr);
				}
			}
			return result;
		} else {
			ASTNode grpByExprs = parseInfo.getGroupByForClause(dest);
			List<ASTNode> result = new ArrayList<ASTNode>(
					grpByExprs == null ? 0 : grpByExprs.getChildCount());
			if (grpByExprs != null) {
				for (int i = 0; i < grpByExprs.getChildCount(); ++i) {
					ASTNode grpbyExpr = (ASTNode) grpByExprs.getChild(i);
					if (grpbyExpr.getType() != HiveParser.TOK_GROUPING_SETS_EXPRESSION) {
						result.add(grpbyExpr);
					}
				}
			}
			return result;
		}
	}

	public static String generateErrorMessage(ASTNode ast, String message) {
		StringBuilder sb = new StringBuilder();
		sb.append(ast.getLine());
		sb.append(":");
		sb.append(ast.getCharPositionInLine());
		sb.append(" ");
		sb.append(message);
		sb.append(". Error encountered near token '");
		sb.append(ErrorMsg.getText(ast));
		sb.append("'");
		return sb.toString();
	}

	private static class SortBucketRSCtx {
		ArrayList<ExprNodeDesc> partnCols;
		boolean multiFileSpray;
		int numFiles;
		int totalFiles;

		public SortBucketRSCtx() {
			partnCols = null;
			multiFileSpray = false;
			numFiles = 1;
			totalFiles = 1;
		}

		/**
		 * @return the partnCols
		 */
		public ArrayList<ExprNodeDesc> getPartnCols() {
			return partnCols;
		}

		/**
		 * @param partnCols
		 *            the partnCols to set
		 */
		public void setPartnCols(ArrayList<ExprNodeDesc> partnCols) {
			this.partnCols = partnCols;
		}

		/**
		 * @return the multiFileSpray
		 */
		public boolean isMultiFileSpray() {
			return multiFileSpray;
		}

		/**
		 * @param multiFileSpray
		 *            the multiFileSpray to set
		 */
		public void setMultiFileSpray(boolean multiFileSpray) {
			this.multiFileSpray = multiFileSpray;
		}

		/**
		 * @return the numFiles
		 */
		public int getNumFiles() {
			return numFiles;
		}

		/**
		 * @param numFiles
		 *            the numFiles to set
		 */
		public void setNumFiles(int numFiles) {
			this.numFiles = numFiles;
		}

		/**
		 * @return the totalFiles
		 */
		public int getTotalFiles() {
			return totalFiles;
		}

		/**
		 * @param totalFiles
		 *            the totalFiles to set
		 */
		public void setTotalFiles(int totalFiles) {
			this.totalFiles = totalFiles;
		}
	}

	private static int getPositionFromInternalName(String internalName) {
		return HiveConf.getPositionFromInternalName(internalName);
	}
}
