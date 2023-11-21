package com.taobao.search.iquan.core.catalog;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.FunctionNotExistException;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.catalog.function.IquanFunction;
import com.taobao.search.iquan.core.catalog.function.IquanStdOperatorTable;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IquanCalciteCatalogReader extends CalciteCatalogReader {
    private static final Logger logger = LoggerFactory.getLogger(IquanCalciteCatalogReader.class);
    private final GlobalCatalogManager catalogManager;
    private final String defaultCatalogName;
    private final String defaultDbName;

    public IquanCalciteCatalogReader(CalciteSchema rootSchema,
                                     List<String> defaultSchema,
                                     RelDataTypeFactory typeFactory,
                                     CalciteConnectionConfig config,
                                     GlobalCatalogManager catalogManager,
                                     String defaultCatalogName,
                                     String defaultDbName) {
        super(rootSchema, defaultSchema, typeFactory, config);
        this.catalogManager = catalogManager;
        this.defaultCatalogName = defaultCatalogName;
        this.defaultDbName = defaultDbName;
    }

    public IquanCalciteCatalogReader(CalciteSchema rootSchema,
                                     SqlNameMatcher nameMatcher, List<List<String>> schemaPaths,
                                     RelDataTypeFactory typeFactory, CalciteConnectionConfig config,
                                     GlobalCatalogManager catalogManager,
                                     String defaultCatalogName,
                                     String defaultDbName) {
        super(rootSchema, nameMatcher, schemaPaths, typeFactory, config);
        this.catalogManager = catalogManager;
        this.defaultCatalogName = defaultCatalogName;
        this.defaultDbName = defaultDbName;
    }

    protected static SqlFunctionCategory category(SqlOperator operator) {
        if (operator instanceof SqlFunction) {
            return ((SqlFunction) operator).getFunctionType();
        } else {
            return SqlFunctionCategory.SYSTEM;
        }
    }

    @Override
    public void lookupOperatorOverloads(SqlIdentifier opName,
                                        SqlFunctionCategory category,
                                        SqlSyntax syntax,
                                        List<SqlOperator> operatorList,
                                        SqlNameMatcher nameMatcher) {
        if (opName.isStar()) {
            return;
        }

        String catalogName = defaultCatalogName;
        String dbName = defaultDbName;
        List<String> namePath = opName.names;
        if (!nameMatcher.isCaseSensitive()) {
            namePath = namePath.stream().map(String::toLowerCase).collect(Collectors.toList());
            catalogName = catalogName.toLowerCase();
            dbName = dbName.toLowerCase();
        }
        int nameSize = namePath.size();
        String funcName = namePath.get(nameSize - 1);

        switch (nameSize) {
            case 3 : catalogName = namePath.get(0);
            case 2 : dbName = namePath.get(nameSize - 2);
            case 1 : break;
            default :
                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_INVALID_FUNCTION_PATH,
                        String.format("function path [%s] is invalid", opName.toString()));
        }
        try {
            if (IquanStdOperatorTable.BUILD_IN_FUNC_MAP.containsKey(funcName)) {
                if (operatorList.isEmpty()) {
                    operatorList.add(IquanStdOperatorTable.BUILD_IN_FUNC_MAP.get(funcName));
                }
                return;
            }
            GlobalCatalog catalog = catalogManager.getCatalog(catalogName);
            IquanFunction iquanFunction = catalog.getFunction(dbName, funcName);
            SqlFunction sqlFunction = iquanFunction.build();
            if (sqlFunction.getSyntax() != syntax) {
                return;
            }
            if (category != null
                && category != category(sqlFunction)
                && !category.isUserDefinedNotSpecificFunction()) {
                return;
            }
            operatorList.add(sqlFunction);
        } catch (FunctionNotExistException e) {
            logger.warn(String.format("function [%s] not exist!", funcName));
        }
    }

    @Override
    public List<SqlOperator> getOperatorList() {
        List<SqlOperator> operators = new ArrayList<>();
        for (GlobalCatalog catalog : catalogManager.getCatalogMap().values()) {
            for (IquanDatabase dataBase : catalog.getDatabases().values()) {
                operators.addAll(dataBase.getFunctions().values()
                        .stream()
                        .map(IquanFunction::build)
                        .collect(Collectors.toList()));
            }
        }
        operators.addAll(IquanStdOperatorTable.instance().getOperatorList());
        return operators;
    }
}
