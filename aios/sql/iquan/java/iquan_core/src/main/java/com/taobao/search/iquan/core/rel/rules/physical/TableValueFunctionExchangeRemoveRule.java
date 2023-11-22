package com.taobao.search.iquan.core.rel.rules.physical;

import com.taobao.search.iquan.core.rel.ops.physical.IquanExchangeOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableFunctionScanOp;
import com.taobao.search.iquan.core.rel.ops.physical.Scope;
import com.taobao.search.iquan.core.rel.visitor.relvisitor.ExchangeVisitor;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableValueFunctionExchangeRemoveRule extends RelOptRule {
    public static final TableValueFunctionExchangeRemoveRule INSTANCE =
            new TableValueFunctionExchangeRemoveRule(RelFactories.LOGICAL_BUILDER);
    private static final Logger logger = LoggerFactory.getLogger(TableValueFunctionExchangeRemoveRule.class);

    private TableValueFunctionExchangeRemoveRule(RelBuilderFactory relBuilderFactory) {
        super(operand(
                IquanTableFunctionScanOp.class,
                operand(
                        IquanExchangeOp.class,
                        operand(
                                IquanTableFunctionScanOp.class,
                                any()
                        )
                )
        ), relBuilderFactory, null);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        IquanTableFunctionScanOp finalFunctionScan = call.rel(0);
        IquanTableFunctionScanOp partialFunctionScan = call.rel(2);

        if (finalFunctionScan.getScope() != Scope.FINAL
                || partialFunctionScan.getScope() != Scope.PARTIAL) {
            return false;
        }

        ExchangeVisitor visitor = new ExchangeVisitor();
        visitor.go(partialFunctionScan);
        return visitor.existExchange();
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        IquanTableFunctionScanOp finalFunctionScan = call.rel(0);
        IquanTableFunctionScanOp partialFunctionScan = call.rel(2);

        IquanTableFunctionScanOp newFunctionScan = new IquanTableFunctionScanOp(
                finalFunctionScan.getCluster(),
                finalFunctionScan.getTraitSet(),
                partialFunctionScan.getInputs(),
                finalFunctionScan.getCall(),
                finalFunctionScan.getElementType(),
                finalFunctionScan.getRowType(),
                finalFunctionScan.getColumnMappings(),
                Scope.NORMAL,
                partialFunctionScan.isBlock(),
                partialFunctionScan.isEnableShuffle()
        );
        call.transformTo(newFunctionScan);
    }
}
