package com.taobao.search.iquan.core.rel.rules.physical.join_utils;

import com.taobao.search.iquan.core.api.schema.IquanTable;
import com.taobao.search.iquan.core.rel.ops.physical.IquanJoinOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanNestedLoopJoinOp;
import org.apache.calcite.rel.RelNode;

public class LookupJoinFactory extends PhysicalJoinFactory {
    final private boolean leftIsBuild;
    final private boolean isInternalBuild;

    public LookupJoinFactory(PhysicalJoinType physicalJoinType, boolean leftIsBuild, boolean isInternalBuild) {
        super(physicalJoinType);
        this.leftIsBuild = leftIsBuild;
        this.isInternalBuild = isInternalBuild;
    }

    @Override
    public RelNode create(IquanJoinOp joinOp, IquanTable leftIquanTable, IquanTable rightIquanTable) {
        return new IquanNestedLoopJoinOp(
                joinOp.getCluster(),
                joinOp.getTraitSet(),
                joinOp.getHints(),
                joinOp.getLeft(),
                joinOp.getRight(),
                joinOp.getCondition(),
                joinOp.getVariablesSet(),
                joinOp.getJoinType(),
                leftIsBuild,
                isInternalBuild);
    }
}
