package com.taobao.search.iquan.core.rel.ops.physical;

import com.taobao.search.iquan.core.rel.ops.physical.IquanCalcOp;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexProgram;

import java.util.List;

public class IquanTurboJetCalcOp extends IquanCalcOp {

    public IquanTurboJetCalcOp(RelOptCluster cluster, RelTraitSet traits, List<RelHint> hints, RelNode child, RexProgram program) {
        super(cluster, traits, hints, child, program);
    }

    @Override
    public Calc copy(RelTraitSet traitSet, RelNode child, RexProgram program) {
        IquanCalcOp rel = new IquanCalcOp(getCluster(), traitSet, getHints(), child, program);
        rel.setParallelNum(getParallelNum());
        rel.setParallelIndex(getParallelIndex());
        return rel;
    }

    @Override
    public String getName() {
        return "TurboJetCalcOp";
    }
}
