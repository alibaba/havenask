package com.taobao.search.iquan.core.rel.rules.physical;

import com.google.common.collect.ImmutableList;
import com.taobao.search.iquan.core.api.schema.Distribution;
import com.taobao.search.iquan.core.catalog.function.internal.TableValueFunction;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.ops.physical.IquanExchangeOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableFunctionScanOp;
import com.taobao.search.iquan.core.rel.visitor.relvisitor.ExchangeVisitor;
import com.taobao.search.iquan.core.utils.RelDistributionUtil;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.tools.RelBuilderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ExecTableValueFunctionRule extends RelOptRule {
    private static final Logger logger = LoggerFactory.getLogger(ExecTableValueFunctionRule.class);
    public static final ExecTableValueFunctionRule INSTANCE =
            new ExecTableValueFunctionRule(RelFactories.LOGICAL_BUILDER);

    private ExecTableValueFunctionRule(RelBuilderFactory relBuilderFactory) {
        super(operand(
                IquanTableFunctionScanOp.class,
                any()
        ), relBuilderFactory, null);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        IquanTableFunctionScanOp functionScan = call.rel(0);

        if (!RelDistributionUtil.isTvfNormalScope(functionScan)) {
            return false;
        }

        SqlOperator operator = ((RexCall) functionScan.getCall()).getOperator();
        if (!(operator instanceof TableValueFunction)) {
            return false;
        }

        TableValueFunction tableValueFunction = (TableValueFunction) operator;
        Map<String, Object> properties = tableValueFunction.getProperties();
        boolean enableShuffle = false;
        Object enableShuffleParam = properties.get(ConstantDefine.ENABLE_SHUFFLE);
        if (enableShuffleParam != null) {
            if (enableShuffleParam instanceof Boolean) {
                enableShuffle = (Boolean) enableShuffleParam;
            } else if (enableShuffleParam instanceof String) {
                enableShuffle = Boolean.parseBoolean((String) enableShuffleParam);
            }
        }
        if (!enableShuffle) {
            return false;
        }

        ExchangeVisitor visitor = new ExchangeVisitor();
        visitor.go(functionScan);
        return !visitor.existExchange();
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        IquanTableFunctionScanOp functionScan = call.rel(0);
        assert functionScan.getInputs().size() == 1;
        RelNode input = functionScan.getInputs().get(0);

        IquanExchangeOp exchangeOp = new IquanExchangeOp(
                input.getCluster(),
                input.getTraitSet().replace(RelDistributions.SINGLETON),
                input,
                Distribution.SINGLETON
        );

        IquanTableFunctionScanOp newFunctionScan = new IquanTableFunctionScanOp(
                functionScan.getCluster(),
                functionScan.getTraitSet(),
                ImmutableList.of(exchangeOp),
                functionScan.getCall(),
                functionScan.getElementType(),
                functionScan.getRowType(),
                functionScan.getColumnMappings(),
                functionScan.getScope(),
                functionScan.isBlock(),
                functionScan.isEnableShuffle()
        );
        call.transformTo(newFunctionScan);
    }
}
