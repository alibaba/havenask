package com.taobao.search.iquan.core.rel.rules.physical;

import com.taobao.search.iquan.core.api.schema.Distribution;
import com.taobao.search.iquan.core.api.schema.Table;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.IquanRelBuilder;
import com.taobao.search.iquan.core.rel.metadata.IquanMetadata;
import com.taobao.search.iquan.core.rel.ops.physical.IquanExchangeOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanJoinOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanNestedLoopJoinOp;
import com.taobao.search.iquan.core.rel.rules.physical.join_utils.RowCountPredicator;
import com.taobao.search.iquan.core.rel.visitor.relvisitor.ExchangeVisitor;
import com.taobao.search.iquan.core.utils.IquanJoinUtils;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.tools.RelBuilderFactory;

public class ExecNonEquiJoinRule extends RelOptRule {
    public static final ExecNonEquiJoinRule INSTANCE = new ExecNonEquiJoinRule(IquanRelBuilder.LOGICAL_BUILDER);

    private ExecNonEquiJoinRule(RelBuilderFactory relBuilderFactory) {
        super(operand(
                IquanJoinOp.class,
                any()
        ), relBuilderFactory, null);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        IquanJoinOp join = call.rel(0);
        JoinRelType joinRelType = join.getJoinType();

        if (IquanJoinUtils.isEquiJoin(join) || !join.getName().equals(ConstantDefine.IQUAN_JOIN)) {
            return false;
        }

        return joinRelType != JoinRelType.FULL;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        IquanJoinOp join = call.rel(0);

        RelNode leftInput = IquanRelOptUtils.toRel(join.getLeft());
        RelNode rightInput = IquanRelOptUtils.toRel(join.getRight());

/*
        boolean leftHasExchange = existExchange(leftInput);
        boolean rightHasExchange = existExchange(rightInput);

        int leftPartitonCnt = getPartitonCnt(leftInput);
        int rightPartitonCnt = getPartitonCnt(rightInput);

        if (leftHasExchange && !rightHasExchange) {
            rightInput = insertExchangeOp(rightInput);
        } else if (!leftHasExchange && rightHasExchange) {
            leftInput = insertExchangeOp(leftInput);
        } else if (!leftHasExchange && !rightHasExchange) {
            if (leftPartitonCnt > 1 && rightPartitonCnt > 1) {
                leftInput = insertExchangeOp(leftInput);
                rightInput = insertExchangeOp(rightInput);
            }
        }
*/

        RowCountPredicator predicator = new RowCountPredicator();
        long leftRowCount = predicator.rowCount(leftInput);
        long rightRowCount = predicator.rowCount(rightInput);
        boolean leftIsBuild = (leftRowCount < rightRowCount);

        IquanJoinOp newJoin = new IquanNestedLoopJoinOp(join.getCluster(), join.getTraitSet(), join.getHints(), leftInput, rightInput,
                join.getCondition(), join.getVariablesSet(), join.getJoinType(), leftIsBuild, true);
        call.transformTo(newJoin);
    }

    /*
    private RelNode insertExchangeOp(RelNode relNode) {
        return new IquanExchangeOp(relNode.getCluster(), relNode.getTraitSet(), relNode, Distribution.SINGLETON);
    }

    private boolean existExchange(RelNode relNode) {
        ExchangeVisitor visitor = new ExchangeVisitor();
        visitor.go(relNode);
        return visitor.existExchange();
    }

    private int getPartitonCnt(RelNode relNode) {
        RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();
        IquanMetadata.ScanNode mdScanNode = relNode.metadata(IquanMetadata.ScanNode.class, mq);
        TableScan tableScan = mdScanNode.getScanNode();
        Table table = IquanRelOptUtils.getIquanTable(tableScan);
        if (table == null) {
            return 1;
        }
        return table.getDistribution().getPartitionCnt();
    }
    */
}
