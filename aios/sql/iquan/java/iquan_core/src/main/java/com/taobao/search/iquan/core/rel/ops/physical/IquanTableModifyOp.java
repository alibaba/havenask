package com.taobao.search.iquan.core.rel.ops.physical;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.config.IquanConfigManager;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.api.schema.Distribution;
import com.taobao.search.iquan.core.api.schema.FieldMeta;
import com.taobao.search.iquan.core.api.schema.IquanTable;
import com.taobao.search.iquan.core.api.schema.Location;
import com.taobao.search.iquan.core.catalog.GlobalCatalog;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.plan.PlanWriteUtils;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.externalize.RelEnumTypes;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;

public class IquanTableModifyOp extends AbstractRelNode implements IquanRelNode {
    private int parallelNum = -1;
    private int parallelIndex = -1;
    private RelOptTable table;
    private RexProgram rexProgram;
    private TableModify.Operation operation;
    private List<String> updateColumnList;
    private List<RexNode> sourceExpressionList;
    private boolean flattened;

    public IquanTableModifyOp(RelOptCluster cluster,
                              RelTraitSet traitSet,
                              RelOptTable table,
                              RexProgram rexProgram,
                              TableModify.Operation operation,
                              List<String> updateColumnList,
                              List<RexNode> sourceExpressionList,
                              boolean flattened) {
        super(cluster, traitSet);
        this.table = table;
        this.rexProgram = rexProgram;
        this.operation = operation;
        this.updateColumnList = updateColumnList;
        this.sourceExpressionList = sourceExpressionList;
        this.flattened = flattened;
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        IquanTableModifyOp rel = new IquanTableModifyOp(getCluster(),
                traitSet,
                table,
                rexProgram,
                operation,
                updateColumnList,
                sourceExpressionList,
                flattened);
        rel.setParallelNum(parallelNum);
        rel.setParallelIndex(parallelIndex);
        return rel;
    }

    @Override
    public String getName() {
        return "TableModifyOp";
    }

    @Override
    public Location getLocation() {
        return null;
    }

    @Override
    public void setLocation(Location location) {

    }

    @Override
    public Distribution getOutputDistribution() {
        return null;
    }

    @Override
    public void setOutputDistribution(Distribution distribution) {

    }

    @Override
    public IquanRelNode deriveDistribution(List<RelNode> inputs, GlobalCatalog catalog, IquanConfigManager config) {
        return null;
    }

    @Override
    protected RelDataType deriveRowType() {
        return RelOptUtil.createDmlRowType(
                SqlKind.INSERT, getCluster().getTypeFactory());
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw)
                .item("table", table.getQualifiedName())
                //.item("operation", RelEnumTypes.fromEnum(operation))
                .itemIf("updateColumnList", updateColumnList, updateColumnList != null)
                .itemIf("sourceExpressionList", sourceExpressionList,
                        sourceExpressionList != null)
                .item("flattened", flattened);
        final Map<String, Object> map = new TreeMap<>();
        SqlExplainLevel level = pw.getDetailLevel();

        IquanRelNode.explainIquanRelNode(this, map, level);
        explainInternal(map, level);

        pw.item(ConstantDefine.ATTRS, map);
        return pw;
    }

    @Override
    public void explainInternal(final Map<String, Object> map, SqlExplainLevel level) {
        IquanConfigManager conf = IquanRelOptUtils.getConfigFromRel(this);
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.OUTPUT_FIELDS, PlanWriteUtils.formatRowFieldName(getRowType()));
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.CONDITION, PlanWriteUtils.formatCondition(rexProgram));
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.OUTPUT_FIELDS_TYPE, PlanWriteUtils.formatRowFieldType(getRowType()));
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.MATCH_TYPE, PlanWriteUtils.formatMatchType(this));
        switch (operation) {
            case INSERT:
                IquanRelOptUtils.addMapIfNotEmpty(
                        map,
                        ConstantDefine.MODIFY_FIELD_EXPRS,
                        PlanWriteUtils.formatOutputRowExpr(rexProgram));
                break;
            case UPDATE:
                IquanRelOptUtils.addMapIfNotEmpty(
                        map,
                        ConstantDefine.MODIFY_FIELD_EXPRS,
                        PlanWriteUtils.formatUpdateExpr(
                                rexProgram.getInputRowType(),
                                updateColumnList,
                                sourceExpressionList));
            case DELETE:
                break;
            default:
                throw new SqlQueryException(
                        IquanErrorCode.IQUAN_EC_UNSUPPORTED_TABLE_MODIFY,
                        "operation " + operation.name());
        }

        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.OPERATION, RelEnumTypes.fromEnum(operation));
        if (level == SqlExplainLevel.ALL_ATTRIBUTES) {
            IquanTable iquanTable = IquanRelOptUtils.getIquanTable(table);
            Distribution distribution = iquanTable.getDistribution();
            Map<String, FieldMeta> primaryMap = iquanTable.getPrimaryMap();
            IquanRelOptUtils.addMapIfNotEmpty(
                    map,
                    ConstantDefine.TABLE_DISTRIBUTION,
                    PlanWriteUtils.formatTableDistribution(distribution, primaryMap, rexProgram, operation, getCluster().getRexBuilder()));
            PlanWriteUtils.formatTableInfo(map, table, conf);
            if (!primaryMap.isEmpty()) {
                IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.PK_FIELD, primaryMap.keySet().iterator().next().substring(1));
            }
        }
    }
}
