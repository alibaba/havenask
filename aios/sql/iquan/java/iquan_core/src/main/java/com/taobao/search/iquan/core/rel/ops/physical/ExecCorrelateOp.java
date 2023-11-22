package com.taobao.search.iquan.core.rel.ops.physical;

import java.util.ArrayList;
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
import com.taobao.search.iquan.core.utils.RelDistributionUtil;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlExplainLevel;

public class ExecCorrelateOp extends SingleRel implements IquanRelNode {
    private final RelDataType outputRowType;
    private final List<IquanUncollectOp> uncollectOps;
    private RexProgram rexProgram;
    private int parallelNum = -1;
    private int parallelIndex = -1;
    private Location location;
    private Distribution distribution;

    public ExecCorrelateOp(RelOptCluster cluster,
                           RelTraitSet traitSet,
                           RelNode input,
                           RelDataType outputRowType,
                           List<IquanUncollectOp> uncollectOps,
                           RexProgram rexProgram) {
        super(cluster, traitSet, input);
        this.outputRowType = outputRowType;
        this.uncollectOps = uncollectOps;
        this.rexProgram = rexProgram;

        assert outputRowType != null;
        assert uncollectOps != null;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        List<IquanUncollectOp> newUncollectOps = new ArrayList<>(uncollectOps.size());
        for (IquanUncollectOp uncollectOp : uncollectOps) {
            newUncollectOps.add((IquanUncollectOp) uncollectOp.copy(traitSet, uncollectOp.getInputs()));
        }

        ExecCorrelateOp rel = new ExecCorrelateOp(getCluster(), traitSet, sole(inputs), outputRowType, newUncollectOps, rexProgram);
        rel.setParallelNum(parallelNum);
        rel.setParallelIndex(parallelIndex);
        return rel;
    }

    public RelNode copy(List<IquanUncollectOp> uncollectOps, RexProgram rexProgram) {
        ExecCorrelateOp rel = new ExecCorrelateOp(getCluster(), traitSet, input, outputRowType, uncollectOps, rexProgram);
        rel.setParallelNum(parallelNum);
        rel.setParallelIndex(parallelIndex);
        return rel;
    }

    public List<IquanUncollectOp> getUncollectOps() {
        return uncollectOps;
    }

    public RexProgram getRexProgram() {
        return rexProgram;
    }

    public void setRexProgram(RexProgram rexProgram) {
        if (rexProgram != null) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_CORRELATE_EMPTY_REXPROGRAM, "");
        }
        this.rexProgram = rexProgram;
    }

    @Override
    public RelDataType deriveRowType() {
        if (rexProgram != null) {
            return rexProgram.getOutputRowType();
        }
        return outputRowType;
    }

    @Override
    public void explainInternal(final Map<String, Object> map, SqlExplainLevel level) {
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.OUTPUT_FIELDS, PlanWriteUtils.formatRowFieldName(getRowType()));
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.CONDITION, PlanWriteUtils.formatCondition(rexProgram));
        if (level == SqlExplainLevel.ALL_ATTRIBUTES) {
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.OUTPUT_FIELD_EXPRS, PlanWriteUtils.formatOutputRowExpr(rexProgram));
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.OUTPUT_FIELDS_TYPE, PlanWriteUtils.formatRowFieldType(getRowType()));
        }

        final List<Object> uncollectAttrsList = new ArrayList<>(uncollectOps.size());
        for (IquanUncollectOp uncollectOp : uncollectOps) {
            final Map<String, Object> attrs = new TreeMap<>();
            uncollectOp.explainInternal(attrs, level);
            uncollectAttrsList.add(attrs);
        }
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.UNCOLLECT_ATTRS, uncollectAttrsList);
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
        List<IquanUncollectOp> newUncollectOps = new ArrayList<>();
        for (IquanUncollectOp uncollectOp : uncollectOps) {
            newUncollectOps.add((IquanUncollectOp) uncollectOp.accept(shuttle));
        }
        RexProgram newProgram = RexShuttleUtils.visit(shuttle, rexProgram);

        boolean change = false;
        for (int i = 0; i < newUncollectOps.size(); ++i) {
            if (newUncollectOps.get(i) != uncollectOps.get(i)) {
                change = true;
                break;
            }
        }
        if (newProgram != rexProgram) {
            change = true;
        }

        if (!change) {
            return this;
        } else {
            return copy(newUncollectOps, newProgram);
        }
    }

    @Override
    public void acceptForTraverse(RexShuttle shuttle) {
        for (IquanUncollectOp uncollectOp : uncollectOps) {
            uncollectOp.acceptForTraverse(shuttle);
        }
        RexShuttleUtils.visitForTraverse(shuttle, rexProgram);
    }

    @Override
    public String getName() {
        return "ExecCorrelateOp";
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
        IquanRelNode input = RelDistributionUtil.checkIquanRelType(inputs.get(0));
        return simpleRelDerive(input);
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
