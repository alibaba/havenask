package com.taobao.search.iquan.core.rel.rules.physical;

import java.util.ArrayList;
import java.util.List;

import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.rel.IquanRelBuilder;
import com.taobao.search.iquan.core.rel.convention.IquanConvention;
import com.taobao.search.iquan.core.rel.ops.physical.IquanUncollectOp;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.MultisetSqlType;
import org.apache.calcite.tools.RelBuilderFactory;

public class UncollectMergeRule extends RelOptRule {
    public static UncollectMergeRule INSTANCE = new UncollectMergeRule(IquanRelBuilder.LOGICAL_BUILDER);

    private UncollectMergeRule(RelBuilderFactory relBuilderFactory) {
        super(operand(
                Uncollect.class,
                operand(
                        Calc.class,
                        operand(
                                Values.class,
                                none()
                        )
                )
        ), relBuilderFactory, null);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Calc calc = call.rel(1);

        RelDataType nestType = calc.getRowType();
        if (!nestType.isStruct()) {
            return false;
        }
        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Uncollect uncollect = call.rel(0);
        Calc calc = call.rel(1);

        List<RelDataTypeField> nestFieldList = calc.getRowType().getFieldList();
        List<String> nestFieldNames = new ArrayList<>(nestFieldList.size());
        List<RelDataType> nestFieldTypes = new ArrayList<>(nestFieldList.size());
        List<Integer> nestFieldCount = new ArrayList<>(nestFieldList.size());

        for (RelDataTypeField nestField : nestFieldList) {
            RexLocalRef rexLocalRef = calc.getProgram().getProjectList().get(nestField.getIndex());
            RexNode rexNode = calc.getProgram().getExprList().get(rexLocalRef.getIndex());
            if (!(rexNode instanceof RexFieldAccess)) {
                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_UNCOLLECT_UNSUPPORT_TYPE,
                        String.format("name: %s, type %s", nestField.getName(), nestField.getType().getFullTypeString()));
            }
            nestFieldNames.add(nestField.getName());
            RelDataType type = nestField.getType();
            nestFieldTypes.add(type);
            if ((type instanceof ArraySqlType || type instanceof MultisetSqlType) && type.getComponentType() instanceof BasicSqlType) {
                nestFieldCount.add(1);
            } else if (type instanceof MultisetSqlType && type.getComponentType().isStruct()) {
                nestFieldCount.add(type.getComponentType().getFieldCount());
            } else {
                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_UNCOLLECT_UNSUPPORT_TYPE,
                        String.format("name: %s, type %s", nestField.getName(), nestField.getType().getFullTypeString()));
            }
        }

        RelTraitSet newRelTraits = uncollect.getTraitSet().replace(IquanConvention.PHYSICAL);
        boolean withOrdinality = uncollect.withOrdinality;

        IquanUncollectOp uncollectOp = new IquanUncollectOp(
                uncollect.getCluster(),
                newRelTraits,
                nestFieldNames,
                nestFieldTypes,
                nestFieldCount,
                withOrdinality,
                uncollect.getRowType(),
                null
        );
        call.transformTo(uncollectOp);
    }
}
