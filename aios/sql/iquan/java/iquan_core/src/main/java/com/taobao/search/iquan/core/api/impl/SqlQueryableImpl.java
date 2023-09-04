package com.taobao.search.iquan.core.api.impl;

import com.taobao.kmonitor.KMonitor;
import com.taobao.search.iquan.client.common.metrics.QueryMetricsReporter;
import com.taobao.search.iquan.core.api.SqlQueryable;
import com.taobao.search.iquan.core.api.SqlTranslator;
import com.taobao.search.iquan.core.api.common.*;
import com.taobao.search.iquan.core.api.config.IquanConfigManager;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.catalog.*;
import com.taobao.search.iquan.core.rel.IquanRuleListener;
import com.taobao.search.iquan.core.rel.IquanRuleSuccessListener;
import com.taobao.search.iquan.core.rel.convention.IquanConvention;
import com.taobao.search.iquan.core.rel.programs.CTEOptimizer;
import com.taobao.search.iquan.core.rel.programs.IquanOptContext;
import com.taobao.search.iquan.core.rel.rewrite.post.PostOptPlanner;
import com.taobao.search.iquan.core.rel.rewrite.sql.AliasRewrite;
import com.taobao.search.iquan.core.rel.rewrite.sql.LimitRewrite;
import com.taobao.search.iquan.core.rel.visitor.relshuttle.*;
import com.taobao.search.iquan.core.rel.visitor.rexshuttle.RexOptimizeInfoCollectShuttle;
import com.taobao.search.iquan.core.rel.visitor.rexshuttle.RexOptimizeInfoRewriteShuttle;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import com.taobao.search.iquan.core.api.config.ExecConfigOptions;
import com.taobao.search.iquan.core.api.config.SqlConfigOptions;
import com.taobao.search.iquan.core.rel.programs.IquanPrograms;
import org.javatuples.Pair;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SqlQueryableImpl implements SqlQueryable {
    private static final Logger logger = LoggerFactory.getLogger(SqlQueryableImpl.class);

    private final IquanExecutorFactory executorFactory;
    private final boolean defaultDebug;

    private SqlQueryableImpl(
                             IquanExecutorFactory executorFactory,
                             boolean defaultDebug) {
        assert executorFactory != null;

        this.executorFactory = executorFactory;
        this.defaultDebug = defaultDebug;
    }

    @Override
    public Object select(List<String> sqls,
                         List<List<Object>> dynamicParams,
                         String defCatalogName,
                         String defDbName,
                         IquanConfigManager config,
                         Map<String, Object> infos) {
        try {
            SqlWorkFlow sqlWorkFlow = buildSqlWorkFlow(executorFactory, sqls, dynamicParams, config, defCatalogName, defDbName, infos);
            try {
                return sqlWorkFlow.process();
            } catch (SqlParseException e) {
                throw new RuntimeException(e);
            }
        } finally {
        }
    }

    public SqlWorkFlow buildSqlWorkFlow(IquanExecutorFactory executorFactory,
                                        List<String> sqls,
                                        List<List<Object>> dynamicParams,
                                        IquanConfigManager config,
                                        String defaultCatalogName,
                                        String defaultDbName,
                                        Map<String, Object> infos) throws SqlQueryException
    {
        boolean debug = defaultDebug || config.getBoolean(SqlConfigOptions.IQUAN_OPTIMIZER_DEBUG_ENABLE);
        return new SqlWorkFlow(debug, executorFactory, sqls, dynamicParams, config, defaultCatalogName, defaultDbName, infos);
    }

    public static class Factory implements SqlQueryable.Factory {

        private final boolean debug;

        public Factory(boolean debug) {
            this.debug = debug;
        }

        @Override
        public SqlQueryableImpl create(SqlTranslator sqlTranslator) {
            return new SqlQueryableImpl(
                    sqlTranslator.getExecutorFactory(),
                    debug);
        }
    }

    public static class SqlWorkFlow {
        private final boolean debug;
        private final IquanExecutorFactory.Executor executor;
        private final List<String> sqls;
        private final List<List<Object>> dynamicParams;
        private final String defCatalogName;
        private final String defDbName;
        private final RelOptCluster cluster;
        private final IquanConfigManager config;
        private final Map<String, Object> infos;

        // input params
        private final PlanFormatVersion formatVersion;
        private final PlanFormatType formatType;
        private final boolean enableObject;
        private final boolean outputExecParams;
        private final boolean enableCteOpt;
        private final boolean enableTurboJet;

        // output params
        private final Map<String, Object> execParams = new TreeMap<>();

        // internal variables
        private final Map<String, Object> planMeta = new HashMap<>();
        private final List<SqlNode> sqlNodes = new ArrayList<>();
        private final List<SqlNode> validSqlNodes = new ArrayList<>();
        private final List<RelNode> relNodes = new ArrayList<>();
        private final List<RelNode> optimizedRelNodes = new ArrayList<>();
        private final List<RelNode> finalRelNodes = new ArrayList<>();
        private boolean needCollectOptimizeInfos = false;
        private final boolean needDetailDebugInfos;
        private final IquanOptContext context;
        private final List<IquanRuleListener> listeners;

        public SqlWorkFlow(
                boolean debug,
                IquanExecutorFactory executorFactory,
                List<String> sqls,
                List<List<Object>> dynamicParams,
                IquanConfigManager configManager,
                String defaultCatalogName,
                String defaultDbName,
                Map<String, Object> infos) {
            this.sqls = sqls;
            this.dynamicParams = dynamicParams;
            this.defCatalogName = defaultCatalogName;
            this.defDbName = defaultDbName;
            this.debug = debug;
            this.config = configManager;
            configManager.setString(SqlConfigOptions.IQUAN_DEFAULT_CATALOG_NAME.key(), defCatalogName);
            configManager.setString(SqlConfigOptions.IQUAN_DEFAULT_DB_NAME.key(), defDbName);
            this.executor = executorFactory.create(defCatalogName, defDbName);
            this.context = genIquanOptContext(this.executor, executorFactory.getCatalogManager(), configManager, dynamicParams, planMeta);
            this.executor.setContext(context);
            this.cluster = executor.getDefaultRelOptCluster();
            this.infos = infos;

            String formatVersion = config.getString(SqlConfigOptions.IQUAN_PLAN_FORMAT_VERSION);
            this.formatVersion = PlanFormatVersion.from(formatVersion);
            String formatType = config.getString(SqlConfigOptions.IQUAN_PLAN_FORMAT_TYPE);
            this.formatType = PlanFormatType.from(formatType);
            this.enableObject = config.getBoolean(SqlConfigOptions.IQUAN_PLAN_FORMAT_OBJECT_ENABLE);
            this.outputExecParams = config.getBoolean(SqlConfigOptions.IQUAN_PLAN_OUTPUT_EXEC_PARAMS);
            this.enableCteOpt = config.getBoolean(SqlConfigOptions.IQUAN_OPTIMIZER_CTE_ENABLE);
            this.enableTurboJet = config.getBoolean(SqlConfigOptions.IQUAN_OPTIMIZER_TURBOJET_ENABLE);
            this.needDetailDebugInfos = debug && config.getBoolean(SqlConfigOptions.IQUAN_OPTIMIZER_DETAIL_DEBUG_ENABLE);
            this.listeners = needDetailDebugInfos ? Collections.singletonList(new IquanRuleSuccessListener()) : Collections.emptyList();

            if (debug && enableCteOpt) {
                logger.info("cte optimize enabled");
            }
        }

        // only for test
        public List<RelNode> getFinalRelNodes() {
            return finalRelNodes;
        }

        public Object process() throws SqlParseException {
            // Init Phase

            // Sql Parse Phase
            long startSqlParseUs = System.nanoTime();
            parseSql();

            long startValidateUs = System.nanoTime();
            validateSql();

            // Rel Transform Phase
            long startTransformUs = System.nanoTime();
            sqlToRelNodes();

            // Rel Optimize Phase
            long startOptimizeUs = System.nanoTime();
            relOptimize();

            // Rel Post Optimize Phase
            long startPostOptimizeUs = System.nanoTime();
            relPostOptimize();

            if (needDetailDebugInfos) {
                listeners.forEach(v -> logger.info(v.display()));
            }

            Map<String, List<Object>> optimizeInfos;
            if (needCollectOptimizeInfos) {
                RexOptimizeInfoCollectShuttle rexShuttle = new RexOptimizeInfoCollectShuttle();
                OptimizeInfoCollectShuttle relShuttle = new OptimizeInfoCollectShuttle(rexShuttle);
                for (int i = 0; i < finalRelNodes.size(); ++i) {
                    rexShuttle.setIndex(i);
                    finalRelNodes.get(i).accept(relShuttle);
                }
                optimizeInfos = rexShuttle.getOptimizeInfos();
            } else {
                optimizeInfos = new TreeMap<>();
            }

            // End Phase
            long endTimeUs = System.nanoTime();
            KMonitor kMonitor = QueryMetricsReporter.getkMonitor();
            if (kMonitor != null) {
                kMonitor.report(QueryMetricsReporter.Parse_LatencyUs, (startValidateUs - startSqlParseUs) / 1000.0);
                kMonitor.report(QueryMetricsReporter.Validate_LatencyUs, (startTransformUs - startValidateUs) / 1000.0);
                kMonitor.report(QueryMetricsReporter.Transform_LatencyUs, (startOptimizeUs - startTransformUs) / 1000.0);
                kMonitor.report(QueryMetricsReporter.Optimize_LatencyUs, (startPostOptimizeUs - startOptimizeUs) / 1000.0);
                kMonitor.report(QueryMetricsReporter.Post_Optimize_LatencyUs, (endTimeUs - startPostOptimizeUs) / 1000.0);
                kMonitor.report(QueryMetricsReporter.Cache_Miss_Qps, 1.0);
            }

            if (outputExecParams) {
                config.setString(ExecConfigOptions.SQL_OPTIMIZER_SQL_PARSE_TIME_US, String.valueOf((startValidateUs - startSqlParseUs) / 1000));
                config.setString(ExecConfigOptions.SQL_OPTIMIZER_SQL_VALIDATE_TIME_US, String.valueOf((startTransformUs - startValidateUs) / 1000));
                config.setString(ExecConfigOptions.SQL_OPTIMIZER_REL_TRANSFORM_TIME_US, String.valueOf((startOptimizeUs - startTransformUs) / 1000));
                config.setString(ExecConfigOptions.SQL_OPTIMIZER_REL_OPTIMIZE_TIME_US, String.valueOf((startPostOptimizeUs - startOptimizeUs) / 1000));
                config.setString(ExecConfigOptions.SQL_OPTIMIZER_REL_POST_OPTIMIZE_TIME_US, String.valueOf((endTimeUs - startPostOptimizeUs) / 1000));

                ExecConfigOptions.addToMap(config, execParams);
            }

            if (debug && infos != null) {
                infos.put(ConstantDefine.SQL_NODES_KEY, sqlNodes.stream().map(SqlNode::toString).collect(Collectors.toList()));
                infos.put(ConstantDefine.VALID_SQL_NODES_KEY, validSqlNodes.stream().map(SqlNode::toString).collect(Collectors.toList()));
                infos.put(ConstantDefine.REL_NODES_KEY, relNodes.stream().map(
                        r -> IquanRelOptUtils.relToString(r, SqlExplainLevel.DIGEST_ATTRIBUTES, true, false, true, Collections.emptyList())
                ).collect(Collectors.toList()));
                infos.put(ConstantDefine.OPTIMIZED_REL_NODES_KEY, IquanRelOptUtils.toPlan(enableObject, formatVersion, formatType, optimizedRelNodes, null, null, planMeta));
                infos.put(ConstantDefine.FINAL_REL_NODES_KEY, IquanRelOptUtils.toPlan(enableObject, formatVersion, formatType, finalRelNodes, null, null, planMeta));
            }

            return IquanRelOptUtils.toPlan(enableObject, formatVersion, formatType, finalRelNodes, execParams, optimizeInfos, planMeta);
        }

        private void parseSql() throws SqlParseException {
            if (!sqlNodes.isEmpty()) {
                return;
            }

            for (String sql : sqls) {
                SqlNode sqlNode = executor.parseSql(sql);

                // add limit
                LimitRewrite limitRule = new LimitRewrite(config);
                sqlNode = sqlNode.accept(limitRule);

                // add alias
                AliasRewrite aliasRule = new AliasRewrite();
                assert sqlNode != null;
                sqlNode = sqlNode.accept(aliasRule);

                sqlNodes.add(sqlNode);
            }
        }

        private void validateSql() {
            if (sqlNodes.isEmpty()) {
                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_WORKFLOW_EMPTY_SQLNODE, "");
            } else if (!validSqlNodes.isEmpty()) {
                return;
            }

            if (sqlNodes.stream().anyMatch(node -> !node.getKind().belongsTo(SqlTranslator.supportSqlKinds))) {
                StringBuilder sqlNodeTypes = new StringBuilder("sql types is ");
                for (SqlNode node : sqlNodes) {
                    sqlNodeTypes.append(node.getKind()).append(",");
                }

                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_WORKFLOW_UNSUPPORT_SQL_TYPE,
                        sqlNodeTypes.toString());
            }

            for (SqlNode node : sqlNodes) {
                SqlNode validated = executor.validate(node);
                validSqlNodes.add(validated);
            }
        }

        private void sqlToRelNodes() {
            if (validSqlNodes.isEmpty()) {
                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_WORKFLOW_EMPTY_SQLNODE, "valid sql nodes is empty.");
            } else if (!relNodes.isEmpty()) {
                return;
            }

            for (SqlNode node : validSqlNodes) {
                RelNode relNode = executor.sql2rel(node);
                relNodes.add(relNode);
            }
        }

        private void relOptimize() {
            if (relNodes.isEmpty()) {
                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_WORKFLOW_EMPTY_RAW_RELNODE, "");
            } else if (!optimizedRelNodes.isEmpty()) {
                return;
            }

            final RelNode rootNode = relNodes.get(0);

            RelNode optNode = relOptimize(rootNode, context);
            // Iquan: Groupping Sets will produce same rel object with multiple parents, will remove later
            //
            // Rewrite same rel object to different rel objects
            // in order to get the correct dag (dag reuse is based on object not digest)
            RelShuttle cteShuttle = enableCteOpt ? new SameRelUniqShuttle() : new SameRelExpandShuttle();
            optimizedRelNodes.add(optNode.accept(cteShuttle));
        }

        private static RelNode relOptRuleApply(RelNode root,
                                               Pair<String, HepProgram> programPair,
                                               Context context,
                                               RelTrait[] relTraits,
                                               String stageName,
                                               List<IquanRuleListener> listeners,
                                               boolean debug) {
            HepPlanner planner = new HepPlanner(programPair.getValue1(), context);
            listeners.forEach(planner::addListener);
            planner.setRoot(root);
            if (relTraits != null) {
                RelTraitSet targetTraitsSet = root
                        .getTraitSet()
                        .plusAll(relTraits);
                if (!root.getTraitSet().equals(targetTraitsSet)) {
                    planner.changeTraits(root, targetTraitsSet.simplify());
                }
            }
            long startUs = System.nanoTime();
            root = planner.findBestExp();
            long endUs = System.nanoTime();
            if (debug) {
                String header = "[" + stageName + "]" + "---" +
                        programPair.getValue0() + "---time cost " +
                        (endUs - startUs) / 1000 + "us";
                dumpPlan(header, root);
            }
            return root;
        }

        private static RelNode relOptRuleListApply(RelNode root,
                                                   List<Pair<String, HepProgram>> programList,
                                                   Context context,
                                                   RelTrait[] relTraits,
                                                   String description,
                                                   List<IquanRuleListener> listeners,
                                                   boolean debug) {
            if (debug) {
                dumpPlan("[" + description + "]" + "----init----", root);
            }
            for (Pair<String, HepProgram> pair : programList) {
                root = relOptRuleApply(root, pair, context, relTraits, description, listeners, debug);
            }
            return root;
        }

        private RelNode relOptimize(RelNode rootNode, IquanOptContext context) {
            OptimizeLevel optimizeLevel = OptimizeLevel.from(config.getString(SqlConfigOptions.IQUAN_OPTIMIZER_LEVEL));
            final List<Pair<String, HepProgram>> fuseLayerTablePrepare = IquanPrograms.fuseLayerTableRules();
            final List<Pair<String, HepProgram>> logicalOpt = IquanPrograms.logicalRules(optimizeLevel);
            final List<Pair<String, HepProgram>> physicalConvert = IquanPrograms.physicalConverterRules(optimizeLevel, enableTurboJet);
            final List<Pair<String, HepProgram>> physicalOpt = IquanPrograms.physicalRules(optimizeLevel, enableTurboJet);
            Function<RelNode, RelNode> optimizeFunc = relnode -> {
                relnode = relOptRuleListApply(relnode, fuseLayerTablePrepare, context, null, IquanPrograms.FUSE_LAYER_TABLE_PREPARE, listeners, debug);
                relnode = relOptRuleListApply(relnode, logicalOpt, context, null, IquanPrograms.LOGICAL_OPTIMIZE, listeners, debug);
                return relnode;
            };

            CTEOptimizer cteOpt = new CTEOptimizer(
                    cluster,
                    optimizeFunc,
                    config
            );
            rootNode = cteOpt.optmize(rootNode);
            RelTrait[] requiredTraitsSet = new RelTrait[]{IquanConvention.PHYSICAL};
            rootNode = relOptRuleListApply(rootNode,
                    physicalConvert,
                    context,
                    requiredTraitsSet,
                    IquanPrograms.PHYSICAL_CONVERT,
                    listeners,
                    debug);

            rootNode = relOptRuleListApply(rootNode,
                    physicalOpt,
                    context,
                    requiredTraitsSet,
                    IquanPrograms.PHYSICAL_OPTIMIZE,
                    listeners,
                    debug);

            return rootNode;
        }

        private void relPostOptimize() {
            if (optimizedRelNodes.isEmpty()) {
                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_WORKFLOW_EMPTY_OPT_RELNODE, "");
            } else if (!finalRelNodes.isEmpty()) {
                return;
            }
            List<RelNode> nodes = new ArrayList<>(optimizedRelNodes.size());
            String currentCatalogName = executor.getDefaultCatalogName();
            String currentDbName = executor.getDefaultDbName();
            PostOptPlanner planner = new PostOptPlanner();
            nodes.addAll(planner.optimize(optimizedRelNodes, context.getCatalogManager().getCatalog(currentCatalogName), currentDbName, config));
            if (debug) {
                for (RelNode node : nodes) {
                    logger.info("optimize ===== post optimize =====\n" +
                            "optimize result: \n" +
                            IquanRelOptUtils.relToString(node,
                                    SqlExplainLevel.EXPPLAN_ATTRIBUTES,
                                    false,
                                    false,
                                    true,
                                    Collections.emptyList()));
                }
            }

            RexOptimizeInfoRewriteShuttle rexShuttle = new RexOptimizeInfoRewriteShuttle(cluster.getRexBuilder());
            OptimizeInfoRewriteShuttle relShuttle = new OptimizeInfoRewriteShuttle(rexShuttle);
            for (RelNode node : nodes) {
                finalRelNodes.add(node.accept(relShuttle));
            }
            needCollectOptimizeInfos = (rexShuttle.getRewriteNum() > 0);
        }

        public static IquanOptContext genIquanOptContext(
                IquanExecutorFactory.Executor executor,
                GlobalCatalogManager manager,
                IquanConfigManager config,
                List<List<Object>> dynamicParams,
                Map<String, Object> planMeta) {
            IquanOptContext context = new IquanOptContext(dynamicParams) {
                @Override
                public <C> C unwrap(Class<C> aClass) {
                    if (aClass.isInstance(this)) {
                        return aClass.cast(this);
                    }
                    else {
                        return null;
                    }
                }

                @Override
                public GlobalCatalogManager getCatalogManager() {
                    return manager;
                }

                @Override
                public IquanConfigManager getConfiguration() {
                    return config;
                }

                @Override
                public IquanExecutorFactory.Executor getExecutor() {
                    return executor;
                }
            };
            context.setPlanMeta(planMeta);
            return context;
        }

        private static void dumpPlan(String header, RelNode node) {
            String data = IquanRelOptUtils.relToString(node);
            logger.info(String.format("%s----- \n%s", header, data));
        }
    }
}
