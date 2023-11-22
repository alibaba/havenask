package com.taobao.search.iquan.core.api.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.taobao.search.iquan.core.api.exception.IquanNotValidateException;
import com.taobao.search.iquan.core.api.schema.IquanSimpleSchema;
import com.taobao.search.iquan.core.calcite.IquanSimpleCalciteSchema;
import com.taobao.search.iquan.core.catalog.GlobalCatalog;
import com.taobao.search.iquan.core.catalog.GlobalCatalogManager;
import com.taobao.search.iquan.core.catalog.IquanCalciteCatalogReader;
import com.taobao.search.iquan.core.catalog.IquanDatabase;
import com.taobao.search.iquan.core.catalog.function.IquanStdOperatorTable;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.IquanRelBuilder;
import com.taobao.search.iquan.core.rel.hint.IquanHintInitializer;
import com.taobao.search.iquan.core.rel.metadata.IquanDefaultRelMetadataProvider;
import com.taobao.search.iquan.core.sql2rel.IquanSqlToRelConverter;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;

public class IquanExecutorFactory {
    //    private static final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    public static final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    public static final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    private static final CalciteConnectionConfig calciteConnectionConfig = genConnectionConfig();
    private static final SqlParser.Config parserConfig = SqlParser.config()
            .withLex(Lex.JAVA)
            .withIdentifierMaxLength(256);
    private static final SqlValidator.Config validateConfig = SqlValidator.Config.DEFAULT
            .withIdentifierExpansion(true)
            .withTypeCoercionEnabled(false)
            .withDefaultNullCollation(NullCollation.LOW);

    private static final SqlToRelConverter.Config converterConfig = SqlToRelConverter.config()
            .withTrimUnusedFields(false)
            .withInSubQueryThreshold(Integer.MAX_VALUE)
            .withExpand(false)
            .withHintStrategyTable(IquanHintInitializer.hintStrategyTable)
            .withDecorrelationEnabled(false)
            .withRelBuilderFactory(IquanRelBuilder.proto(Contexts.EMPTY_CONTEXT))
            .withRelBuilderConfigTransform(c -> c.withPushJoinCondition(true).withBloat(-1).withSimplify(true));

    private static final DataContext dataContext = new DataContext() {
        @Override
        public SchemaPlus getRootSchema() {
            return null;
        }

        @Override
        public JavaTypeFactory getTypeFactory() {
            return null;
        }

        @Override
        public QueryProvider getQueryProvider() {
            return null;
        }

        @Override
        public Object get(String name) {
            return null;
        }
    };

    private static final RexExecutor rexExecutor = new RexExecutorImpl(dataContext);
    private static final List<RelTraitDef> traitDefs = Arrays.asList(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE);
    private final GlobalCatalogManager catalogManager;
    private final Map<String, Prepare.CatalogReader> catalogReaderMap;
    private FrameworkConfig frameworkConfig;

    public IquanExecutorFactory(GlobalCatalogManager catalogManager) {
        this.catalogManager = catalogManager;
        frameworkConfig = Frameworks.newConfigBuilder()
                .parserConfig(parserConfig)
                .sqlValidatorConfig(validateConfig)
                .sqlToRelConverterConfig(converterConfig)
                .traitDefs(traitDefs)
                .executor(rexExecutor)
                .build();
        this.catalogReaderMap = genCatalogReaderMap(catalogManager);
    }

    private static CalciteConnectionConfig genConnectionConfig() {
        Properties properties = new Properties();
        properties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
        return new CalciteConnectionConfigImpl(properties);
    }

    private static CalciteCatalogReader genCatalogReader(GlobalCatalogManager catalogManager, String catalogName, String dbName) {
        List<String> defaultSchemaPath = Arrays.asList(catalogName, dbName);
        List<List<String>> paths = IntStream.rangeClosed(0, defaultSchemaPath.size())
                .mapToObj(i -> defaultSchemaPath.subList(0, defaultSchemaPath.size() - i))
                .collect(Collectors.toList());
        return new IquanCalciteCatalogReader(
                genRootSchema(catalogManager, catalogName),
                SqlNameMatchers.withCaseSensitive(calciteConnectionConfig.caseSensitive()),
                paths,
                typeFactory,
                calciteConnectionConfig,
                catalogManager,
                catalogName,
                dbName);
    }

    private static Map<String, Prepare.CatalogReader> genCatalogReaderMap(GlobalCatalogManager catalogManager) {
        Map<String, Prepare.CatalogReader> readerMap = new HashMap<>();
        for (Map.Entry<String, GlobalCatalog> catalogEntry : catalogManager.getCatalogMap().entrySet()) {
            String catalogName = catalogEntry.getKey();
            for (Map.Entry<String, IquanDatabase> dbEntry : catalogEntry.getValue().getDatabases().entrySet()) {
                String dbName = dbEntry.getKey();
                readerMap.put(catalogName + ConstantDefine.PATH_SEPARATOR + dbName, genCatalogReader(catalogManager, catalogName, dbName));
            }
        }
        return readerMap;
    }

    public static CalciteSchema genRootSchema(GlobalCatalogManager manager, String schemaName) {
        GlobalCatalog catalog = manager.getCatalog(schemaName);
        Schema schema = genCatalogSchema(catalog);
        return IquanSimpleCalciteSchema.createRootSchema("", schema);
    }

    private static Schema genCatalogSchema(GlobalCatalog catalog) {
        IquanSimpleSchema schema = new IquanSimpleSchema();
        for (Map.Entry<String, IquanDatabase> dbEntry : catalog.getDatabases().entrySet()) {
            schema.addSubSchema(dbEntry.getKey(), genDataBaseSchema(dbEntry.getValue()));
        }
        IquanSimpleSchema catalogSchema = new IquanSimpleSchema();
        catalogSchema.addSubSchema(catalog.getCatalogName(), schema);
        return catalogSchema;
    }

    private static Schema genDataBaseSchema(IquanDatabase dataBase) {
        IquanSimpleSchema schema = new IquanSimpleSchema();
        schema.setDataBase(dataBase);
        return schema;
    }

    public Executor create(String defaultCatalogName, String defaultDbName) {
        return new Executor(defaultCatalogName, defaultDbName);
    }

    public GlobalCatalogManager getCatalogManager() {
        return catalogManager;
    }

    public class Executor {

        private final String defaultCatalogName;
        private final String defaultDbName;
        private Prepare.CatalogReader catalogReader;
        private SqlParser parser;
        private SqlValidator validator;
        private SqlToRelConverter converter;
        private Context context;
        private RelOptCluster relOptCluster;

        public Executor(String defaultCatalogName, String defaultDbName) {
            this.defaultCatalogName = defaultCatalogName;
            this.defaultDbName = defaultDbName;
            this.catalogReader = catalogReaderMap.get(defaultCatalogName + ConstantDefine.PATH_SEPARATOR + defaultDbName);
            if (catalogReader == null) {
                throw new IquanNotValidateException(
                        String.format("catalog [%s] database[%s] not exits", defaultCatalogName, defaultDbName));
            }
        }

        public SqlNode parseSql(String sql) throws SqlParseException {
            parser = SqlParser.create(sql, frameworkConfig.getParserConfig());
            return parser.parseStmt();
        }

        public SqlNode validate(SqlNode node) {
            final SqlOperatorTable opTab = SqlOperatorTables.chain(SqlStdOperatorTable.instance(), IquanStdOperatorTable.instance(), catalogReader);
            validator = new IquanSqlValidatorImpl(opTab, catalogReader, typeFactory, frameworkConfig.getSqlValidatorConfig());
            return validator.validate(node);
        }

        public RelNode sql2rel(SqlNode node) {
            if (validator == null) {
                node = validate(node);
            }

            converter = new IquanSqlToRelConverter(
                    null,
                    validator,
                    catalogReader,
                    getDefaultRelOptCluster(),
                    frameworkConfig.getConvertletTable(),
                    frameworkConfig.getSqlToRelConverterConfig()
            );
            RelRoot root = converter.convertQuery(node, false, true);
            return root.project();
        }

        public String getDefaultCatalogName() {
            return defaultCatalogName;
        }

        public String getDefaultDbName() {
            return defaultDbName;
        }

        public Context getContext() {
            return context;
        }

        public void setContext(Context context) {
            this.context = context;
            frameworkConfig = Frameworks.newConfigBuilder(frameworkConfig)
                    .context(context)
                    .build();
        }

        public RelOptCluster getDefaultRelOptCluster() {
            if (relOptCluster != null) {
                return relOptCluster;
            }
            VolcanoPlanner planner = new VolcanoPlanner(frameworkConfig.getContext());
            for (RelTraitDef traitDef : frameworkConfig.getTraitDefs()) {
                planner.addRelTraitDef(traitDef);
            }
            planner.setTopDownOpt(true);
            planner.setExecutor(frameworkConfig.getExecutor());

            RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
            cluster.setMetadataProvider(IquanDefaultRelMetadataProvider.INSTANCE);

            relOptCluster = cluster;
            return cluster;
        }
    }
}
