package com.taobao.search.iquan.core.rel.rules.physical.join_utils;

import com.taobao.search.iquan.core.api.schema.IquanTable;
import com.taobao.search.iquan.core.rel.ops.physical.IquanHashJoinOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanJoinOp;
import org.apache.calcite.rel.RelNode;

public class HashJoinFactory extends PhysicalJoinFactory {
    final private boolean leftIsBuild;
    final private boolean tryDistinctBuildRow;

    public HashJoinFactory(PhysicalJoinType physicalJoinType, boolean leftIsBuild, boolean tryDistinctBuildRow) {
        super(physicalJoinType);
        this.leftIsBuild = leftIsBuild;
        this.tryDistinctBuildRow = tryDistinctBuildRow;
    }

    @Override
    public RelNode create(IquanJoinOp joinOp, IquanTable leftIquanTable, IquanTable rightIquanTable) {
        return new IquanHashJoinOp(
                joinOp.getCluster(),
                joinOp.getTraitSet(),
                joinOp.getHints(),
                joinOp.getLeft(),
                joinOp.getRight(),
                joinOp.getCondition(),
                joinOp.getVariablesSet(),
                joinOp.getJoinType(),
                leftIsBuild,
                tryDistinctBuildRow
        );
    }
}
