package com.taobao.search.iquan.core.catalog;

import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.CatalogException;
import com.taobao.search.iquan.core.api.exception.DatabaseNotExistException;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.catalog.function.IquanFunction;
import com.taobao.search.iquan.core.catalog.function.IquanStdOperatorTable;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.validate.SqlNameMatcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IquanCalciteCatalogReader extends CalciteCatalogReader {
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

    @Override
    public void lookupOperatorOverloads(SqlIdentifier opName,
                                        SqlFunctionCategory category,
                                        SqlSyntax syntax,
                                        List<SqlOperator> operatorList,
                                        SqlNameMatcher nameMatcher)
    {
        if (opName.isStar()) {
            return;
        }

        String catalogName = defaultCatalogName;
        String dbName = defaultDbName;
        int nameSize = opName.names.size();

        switch (nameSize) {
            case 3 : catalogName = opName.names.get(0);
            case 2 : dbName = opName.names.get(nameSize - 2);
            case 1 : break;
            default :
                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_INVALID_FUNCTION_PATH,
                        String.format("function path [%s] is invalid", opName.toString()));
        }
        try {
            IquanDataBase dataBase = catalogManager.getCatalog(catalogName).getDatabase(dbName);
            findFunction(dataBase.getFunctions(), opName.names.get(nameSize - 1), category, syntax, operatorList, nameMatcher);
        } catch (DatabaseNotExistException e) {
            throw new RuntimeException(e);
        }
    }

    private void findFunction(Map<String, IquanFunction> functionMap,
                              String funcName,
                              SqlFunctionCategory category,
                              SqlSyntax syntax,
                              List<SqlOperator> operatorList,
                              SqlNameMatcher nameMatcher) {
        for (IquanFunction function : functionMap.values()) {
            String name = function.getName();
            if (!nameMatcher.matches(funcName, name)) {
                continue;
            }
            SqlFunction sqlFunction = function.build();
            if (sqlFunction.getSyntax() != syntax) {
                continue;
            }
            if (category != null
                    && category != category(sqlFunction)
                    && !category.isUserDefinedNotSpecificFunction()) {
                continue;
            }
            operatorList.add(sqlFunction);
        }
    }

    @Override
    public List<SqlOperator> getOperatorList() {
        List<SqlOperator> operators = new ArrayList<>();
        for (GlobalCatalog catalog : catalogManager.getCatalogMap().values()) {
            for (IquanDataBase dataBase : catalog.getDatabases().values()) {
                operators.addAll(dataBase.getFunctions().values()
                        .stream()
                        .map(IquanFunction::build)
                        .collect(Collectors.toList()));
            }
        }
        operators.addAll(IquanStdOperatorTable.instance().getOperatorList());
        return operators;
    }

    protected static SqlFunctionCategory category(SqlOperator operator) {
        if (operator instanceof SqlFunction) {
            return ((SqlFunction) operator).getFunctionType();
        } else {
            return SqlFunctionCategory.SYSTEM;
        }
    }
}
