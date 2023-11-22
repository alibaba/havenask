package com.taobao.search.iquan.core.rel.ops.physical;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.config.IquanConfigManager;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.api.schema.Distribution;
import com.taobao.search.iquan.core.api.schema.Location;
import com.taobao.search.iquan.core.catalog.GlobalCatalog;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.plan.PlanWriteUtils;
import com.taobao.search.iquan.core.rel.visitor.rexshuttle.RexShuttleUtils;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlExplainLevel;

public class IquanUncollectOp extends AbstractRelNode implements IquanRelNode {
    private final List<String> nestFieldNames;
    private final List<RelDataType> nestFieldTypes;
    private final List<Integer> nestFieldCount;
    private final boolean withOrdinality;
    private final RelDataType uncollectRowType;
    private RexProgram rexProgram;
    private int parallelNum = -1;
    private int parallelIndex = -1;
    private Location location;
    private Distribution distribution;

    public IquanUncollectOp(RelOptCluster cluster, RelTraitSet traitSet, List<String> nestFieldNames, List<RelDataType> nestFieldTypes, List<Integer> nestFieldCount,
                            boolean withOrdinality, RelDataType uncollectRowType, RexProgram rexProgram) {
        super(cluster, traitSet);
        this.nestFieldNames = nestFieldNames;
        this.nestFieldTypes = nestFieldTypes;
        this.nestFieldCount = nestFieldCount;
        this.withOrdinality = withOrdinality;
        this.uncollectRowType = uncollectRowType;
        this.rexProgram = rexProgram;

        assert uncollectRowType != null;
        assert nestFieldNames != null && !nestFieldNames.isEmpty();
        assert (nestFieldTypes != null) && (nestFieldTypes.size() == nestFieldNames.size());
        assert (nestFieldCount != null) && (nestFieldCount.size() == nestFieldNames.size());
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        IquanUncollectOp rel = new IquanUncollectOp(getCluster(), traitSet, nestFieldNames, nestFieldTypes, nestFieldCount, withOrdinality, uncollectRowType, rexProgram);
        rel.setParallelNum(parallelNum);
        rel.setParallelIndex(parallelIndex);
        return rel;
    }

    public RelNode copy(RexProgram rexProgram) {
        IquanUncollectOp rel = new IquanUncollectOp(getCluster(), getTraitSet(), nestFieldNames, nestFieldTypes, nestFieldCount, withOrdinality, uncollectRowType, rexProgram);
        rel.setParallelNum(parallelNum);
        rel.setParallelIndex(parallelIndex);
        return rel;
    }

    public List<String> getNestFieldNames() {
        return nestFieldNames;
    }

    public List<RelDataType> getNestFieldTypes() {
        return nestFieldTypes;
    }

    public List<Integer> getNestFieldCount() {
        return nestFieldCount;
    }

    public boolean isWithOrdinality() {
        return withOrdinality;
    }

    public RelDataType getUncollectRowType() {
        return uncollectRowType;
    }

    public RexProgram getRexProgram() {
        return rexProgram;
    }

    public void setRexProgram(RexProgram rexProgram) {
        if (rexProgram != null) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_UNCOLLECT_EMPTY_REXPROGRAM, "");
        }
        this.rexProgram = rexProgram;
    }

    @Override
    protected RelDataType deriveRowType() {
        if (rexProgram != null) {
            return rexProgram.getOutputRowType();
        }
        return uncollectRowType;
    }

    @Override
    public void explainInternal(final Map<String, Object> map, SqlExplainLevel level) {
        // TODO: compatible, will remove later
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.TABLE_NAME, nestFieldNames.get(0));
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.NEST_FIELD_NAMES, PlanWriteUtils.formatFieldNames(nestFieldNames));
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.NEST_FIELD_COUNTS, nestFieldCount);
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.WITH_ORDINALITY, withOrdinality);
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.OUTPUT_FIELDS, PlanWriteUtils.formatRowFieldName(getRowType()));
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.CONDITION, PlanWriteUtils.formatCondition(rexProgram));
        if (level == SqlExplainLevel.ALL_ATTRIBUTES) {
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.NEST_FIELD_TYPES, PlanWriteUtils.formatRelDataTypes(nestFieldTypes));
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.OUTPUT_FIELD_EXPRS, PlanWriteUtils.formatOutputRowExpr(rexProgram));
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.OUTPUT_FIELDS_TYPE, PlanWriteUtils.formatRowFieldType(getRowType()));
        }
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);

        final Map<String, Object> map = new TreeMap<>();
        SqlExplainLevel level = pw.getDetailLevel();

        IquanRelNode.explainIquanRelNode(this, map, level);
        explainInternal(map, level);

        pw.item(ConstantDefine.ATTRS, map);
        return pw;
    }

    @Override
    public RelNode accept(RexShuttle shuttle) {
        RexProgram newProgram = RexShuttleUtils.visit(shuttle, rexProgram);
        if (newProgram == rexProgram) {
            return this;
        } else {
            return copy(newProgram);
        }
    }

    @Override
    public void acceptForTraverse(RexShuttle shuttle) {
        RexShuttleUtils.visitForTraverse(shuttle, rexProgram);
    }

    @Override
    public String getName() {
        return "UncollectOp";
    }

    @Override
    public Location getLocation() {
        return location;
    }

    @Override
    public void setLocation(Location location) {
        this.location = location;
    }

    @Override
    public Distribution getOutputDistribution() {
        return distribution;
    }

    @Override
    public void setOutputDistribution(Distribution distribution) {
        this.distribution = distribution;
    }

    @Override
    public IquanRelNode deriveDistribution(List<RelNode> inputs, GlobalCatalog catalog, IquanConfigManager config) {
        return null;
    }

    @Override
    public int getParallelNum() {
        return parallelNum;
    }

    @Override
    public void setParallelNum(int parallelNum) {
        this.parallelNum = parallelNum;
    }

    @Override
    public int getParallelIndex() {
        return parallelIndex;
    }

    @Override
    public void setParallelIndex(int parallelIndex) {
        this.parallelIndex = parallelIndex;
    }
}
