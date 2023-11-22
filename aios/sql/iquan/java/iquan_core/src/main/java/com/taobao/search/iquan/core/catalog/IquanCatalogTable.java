package com.taobao.search.iquan.core.catalog;

import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import com.taobao.search.iquan.core.api.exception.CatalogException;
import com.taobao.search.iquan.core.api.schema.AbstractField;
import com.taobao.search.iquan.core.api.schema.AtomicField;
import com.taobao.search.iquan.core.api.schema.IquanTable;
import com.taobao.search.iquan.core.utils.IquanTypeFactory;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;

public class IquanCatalogTable extends AbstractTable implements InitializerExpressionFactory, TranslatableTable {
    protected final IquanTable iquanTable;
    protected final String catalogName;
    protected final String dbName;
    protected String tableName;
    protected RelDataType rowType;

    public IquanCatalogTable(String catalogName, String dbName, String tableName, IquanTable iquanTable) {
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.iquanTable = iquanTable;
        this.tableName = tableName;
    }

    public IquanCatalogTable copyWithName(String newTableName) {
        return new IquanCatalogTable(catalogName, dbName, newTableName, iquanTable);
    }

    protected void setTableName(String newTableName) {
        this.tableName = newTableName;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if (rowType != null) {
            return rowType;
        }
        IquanTypeFactory factory = IquanTypeFactory.DEFAULT;
        List<RelDataType> relTypes = iquanTable.getFields()
                .stream()
                .map(factory::createRelType)
                .collect(Collectors.toList());
        List<String> fieldNames = iquanTable.getFields()
                .stream()
                .map(AbstractField::getFieldName)
                .collect(Collectors.toList());
        rowType = typeFactory.createStructType(relTypes, fieldNames);
        return rowType;
    }

    @Override
    public boolean isGeneratedAlways(RelOptTable table, int iColumn) {
        switch (generationStrategy(table, iColumn)) {
            case VIRTUAL:
            case STORED:
                return true;
            default:
                return false;
        }
    }

    @Override
    public ColumnStrategy generationStrategy(RelOptTable table, int iColumn) {
        return ColumnStrategy.DEFAULT;
    }

    @Override
    public RexNode newColumnDefaultValue(RelOptTable table, int iColumn, InitializerContext context) {
        RelDataType colType = table.getRowType().getFieldList().get(iColumn).getType();
        RexBuilder rexBuilder = context.getRexBuilder();
        AbstractField abstractField = this.iquanTable.getFields().get(iColumn);
        if (!(abstractField instanceof AtomicField)) {
            return rexBuilder.makeNullLiteral(colType);
        }
        Object value = abstractField.getDefaultValue();
        switch (colType.getSqlTypeName()) {
            case DATE: {
                value = new DateString(value.toString());
                break;
            }
            case TIME: {
                value = new TimeString(value.toString());
                break;
            }
            case TIMESTAMP:
                value = new TimestampString(value.toString());
                break;
            default:
                break;
        }
        return rexBuilder.makeLiteral(value, colType, false);
    }

    @Override
    public BiFunction<InitializerContext, RelNode, RelNode> postExpressionConversionHook() {
        return null;
    }

    @Override
    public RexNode newAttributeInitializer(RelDataType type, SqlFunction constructor, int iAttribute, List<RexNode> constructorArgs, InitializerContext context) {
        return null;
    }

    public String getTableName() {
        return tableName;
    }
    public IquanTable getTable() {
        return iquanTable;
    }

    public String getDetailInfo(boolean debug) {
        if (iquanTable == null) {
            return "{}";
        }
        return debug? iquanTable.getDebugDigest() : iquanTable.getDigest();
    }

    public static IquanCatalogTable unwrap(Table table) {
        if (table instanceof IquanCatalogTable) {
            return (IquanCatalogTable) table;
        }
        throw new CatalogException(String.format("Table  cant cast to IquanCatalogTable"));
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        return LogicalTableScan.create(context.getCluster(), relOptTable, context.getTableHints());
    }
}
