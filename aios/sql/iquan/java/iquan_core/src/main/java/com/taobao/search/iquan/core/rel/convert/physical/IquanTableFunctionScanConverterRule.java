package com.taobao.search.iquan.core.rel.convert.physical;

import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.FunctionNotExistException;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.catalog.function.internal.TableValueFunction;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.convention.IquanConvention;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableFunctionScanOp;
import com.taobao.search.iquan.core.rel.ops.physical.Scope;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class IquanTableFunctionScanConverterRule extends ConverterRule {
    private static final Logger logger = LoggerFactory.getLogger(IquanTableFunctionScanConverterRule.class);
    public static IquanTableFunctionScanConverterRule INSTANCE = new IquanTableFunctionScanConverterRule();

    private IquanTableFunctionScanConverterRule() {
        super(LogicalTableFunctionScan.class, Convention.NONE, IquanConvention.PHYSICAL, IquanTableFunctionScanConverterRule.class.getSimpleName());
    }

    @Override
    public RelNode convert(RelNode relNode) {
        final LogicalTableFunctionScan functionScan = (LogicalTableFunctionScan) relNode;
        final RelTraitSet traitSet = functionScan.getTraitSet().replace(IquanConvention.PHYSICAL);

        boolean isNormalScope = true;
        boolean isBlock = false;
        boolean enableShuffle = false;
        SqlOperator operator = ((RexCall) functionScan.getCall()).getOperator();
        if (operator instanceof TableValueFunction) {
            TableValueFunction tableValueFunction = (TableValueFunction) operator;

            if (existNestedTVF((RexCall) functionScan.getCall())) {
                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_TABLE_FUNCTION_SCAN_UNSUPPORT_NESTED_TVF, "");
            }

            Map<String, Object> properties = tableValueFunction.getProperties();
            /*
            Object normalScopeParam = properties.get(ConstantDefine.NORMAL_SCOPE);
            if (normalScopeParam != null) {
                if (normalScopeParam instanceof Boolean) {
                    isNormalScope = (Boolean) normalScopeParam;
                } else if (normalScopeParam instanceof String) {
                    isNormalScope = Boolean.parseBoolean((String) normalScopeParam);
                }
            }
            */
            Object blockParam = properties.get(ConstantDefine.BLOCK);
            if (blockParam != null) {
                if (blockParam instanceof Boolean) {
                    isBlock = (Boolean) blockParam;
                } else if (blockParam instanceof String) {
                    isBlock = Boolean.parseBoolean((String) blockParam);
                }
            }
            Object enableShuffleParam = properties.get(ConstantDefine.ENABLE_SHUFFLE);
            if (enableShuffleParam != null) {
                if (enableShuffleParam instanceof Boolean) {
                    enableShuffle = (Boolean) enableShuffleParam;
                } else if (enableShuffleParam instanceof String) {
                    enableShuffle = Boolean.parseBoolean((String) enableShuffleParam);
                }
            }

            /*
            do {
                if (!isNormalScope && !tableValueFunction.isIdentityFields()) {
                    logger.warn("table value function {} has not identity output fields, force set to normal_score",
                            tableValueFunction.getTvfFunction().getName());
                    isNormalScope = true;
                    break;
                }

                JsonTvfDistribution distribution = tableValueFunction.getDistribution();
                if (distribution.getPartitionCnt() == 1) {
                    logger.warn("partition cnt of table value function {} is 1, force set to normal_score",
                            tableValueFunction.getTvfFunction().getName());
                    isNormalScope = true;
                    break;
                }
            } while (false);
            */
        }
/*
        if (!isNormalScope) {
            IquanTableFunctionScanOp localFunctionScan = new IquanTableFunctionScanOp(
                    functionScan.getCluster(),
                    traitSet,
                    functionScan.getInputs(),
                    functionScan.getCall(),
                    functionScan.getElementType(),
                    functionScan.getRowType(),
                    functionScan.getColumnMappings(),
                    Scope.PARTIAL,
                    isBlock
            );

            IquanExchangeOp exchangeOp = new IquanExchangeOp(
                    functionScan.getCluster(),
                    traitSet.replace(FlinkRelDistribution.SINGLETON()),
                    localFunctionScan,
                    Distribution.SINGLETON
            );

            return new IquanTableFunctionScanOp(
                    functionScan.getCluster(),
                    traitSet,
                    ImmutableList.of(exchangeOp),
                    functionScan.getCall(),
                    functionScan.getElementType(),
                    functionScan.getRowType(),
                    functionScan.getColumnMappings(),
                    Scope.FINAL,
                    true);
        } else {
            return new IquanTableFunctionScanOp(
                    functionScan.getCluster(),
                    traitSet,
                    functionScan.getInputs(),
                    functionScan.getCall(),
                    functionScan.getElementType(),
                    functionScan.getRowType(),
                    functionScan.getColumnMappings(),
                    Scope.NORMAL,
                    isBlock);
        }
*/
        IquanTableFunctionScanOp scanOp = new IquanTableFunctionScanOp(
                functionScan.getCluster(),
                traitSet,
                functionScan.getInputs(),
                functionScan.getCall(),
                functionScan.getElementType(),
                functionScan.getRowType(),
                functionScan.getColumnMappings(),
                Scope.NORMAL,
                isBlock,
                enableShuffle
        );
        try {
            return IquanBuildInConvertor.convertRankTvf(scanOp);
        } catch (FunctionNotExistException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean existNestedTVF(RexCall rootCall) {
        for (RexNode rexNode : rootCall.getOperands()) {
            if (rexNode instanceof RexCall) {
                RexCall call = (RexCall) rexNode;
                if (call.getOperator() instanceof TableValueFunction) {
                    return true;
                }
                if (existNestedTVF(call)) {
                    return true;
                }
            }
        }
        return false;
    }
}
