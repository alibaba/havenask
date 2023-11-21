package com.taobao.search.iquan.core.api.schema;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.taobao.search.iquan.core.api.exception.CatalogException;
import com.taobao.search.iquan.core.catalog.IquanCatalogTable;
import com.taobao.search.iquan.core.catalog.IquanDatabase;
import com.taobao.search.iquan.core.common.ConstantDefine;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;

public class IquanSimpleSchema implements Schema {
    public static final String DEFAULT_TEMPLATE_TABLE_NAME = "__default__";
    private final Map<String, Schema> subSchemaMap = new HashMap<>();
    private IquanDatabase dataBase = null;

    public void addSubSchema(String name, Schema schema) {
        subSchemaMap.put(name, schema);
    }

    public void setDataBase(IquanDatabase dataBase) {
        this.dataBase = dataBase;
    }

    @Override
    public Table getTable(String name) {
        if (dataBase == null) {
            return null;
        }
        if (name.contains(ConstantDefine.TEMPLATE_TABLE_SEPARATION)) {
            return findTemplateTable(name);
        }
        return dataBase.getTable(name);
    }

    @Override
    public Set<String> getTableNames() {
        if (dataBase == null) {
            return Collections.EMPTY_SET;
        }
        return dataBase.getTables().keySet();
    }

    @Override
    public RelProtoDataType getType(String name) {
        return null;
    }

    @Override
    public Set<String> getTypeNames() {
        return Collections.EMPTY_SET;
    }

    @Override
    public Collection<Function> getFunctions(String name) {
        return null;
    }

    @Override
    public Set<String> getFunctionNames() {
        return null;
    }

    @Override
    public Schema getSubSchema(String name) {
        return subSchemaMap.get(name);
    }

    @Override
    public Set<String> getSubSchemaNames() {
        return subSchemaMap.keySet();
    }

    @Override
    public Expression getExpression(SchemaPlus parentSchema, String name) {
        return Schemas.subSchemaExpression(parentSchema, name, getClass());
    }

    @Override
    public boolean isMutable() {
        return true;
    }

    @Override
    public Schema snapshot(SchemaVersion version) {
        return this;
    }

    private Table findTemplateTable(String templateTableName) {
        List<String> nameList = Arrays.asList(templateTableName.split(ConstantDefine.TEMPLATE_TABLE_SEPARATION));
        if (nameList.size() != 2 || nameList.get(1).isEmpty()) {
            String errorMsg = String.format("database %s, table %s is not valid", dataBase.getDbName(), templateTableName);
            throw new CatalogException(errorMsg);
        }
        final String templateName = nameList.get(0).isEmpty() ? DEFAULT_TEMPLATE_TABLE_NAME : nameList.get(0);
        final String tableName = nameList.get(1);

        Table table = getTable(tableName);
        if (table == null) {
            table = getTable(templateName);
            assert table instanceof IquanCatalogTable;
            IquanCatalogTable iquanCatalogTable = (IquanCatalogTable) table;
            return iquanCatalogTable.copyWithName(tableName);
        }
        return table;
    }
}
