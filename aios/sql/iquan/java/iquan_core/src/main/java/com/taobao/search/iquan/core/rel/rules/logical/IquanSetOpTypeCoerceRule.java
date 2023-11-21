package com.taobao.search.iquan.core.rel.rules.logical;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.immutables.value.Value;

@Value.Enclosing
public class IquanSetOpTypeCoerceRule
        extends RelRule<IquanSetOpTypeCoerceRule.Config>
        implements TransformationRule {

    protected IquanSetOpTypeCoerceRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final SetOp setOp = call.rel(0);
        SetOpTypeCoerce converter = new SetOpTypeCoerce(setOp, call.builder());
        if (!converter.needConvert()) {
            return;
        }
        final RelNode newNode = converter.convert();
        call.transformTo(newNode);
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableIquanSetOpTypeCoerceRule.Config.builder().operandSupplier(b0 ->
                        b0.operand(SetOp.class).anyInputs())
                .build();

        @Override
        default IquanSetOpTypeCoerceRule toRule() {
            return new IquanSetOpTypeCoerceRule(this);
        }
    }

    public static class SetOpTypeCoerce {

        final List<RelNode> inputs;
        final List<BitSet> diffSets;
        final List<Boolean> hasDiffs;
        private final SetOp setOp;
        private final RelBuilder relBuilder;
        private final RexBuilder rexBuilder;
        private final List<RelDataTypeField> outputFields;

        public SetOpTypeCoerce(SetOp setOp, RelBuilder relBuilder) {
            this.setOp = setOp;
            this.relBuilder = relBuilder;
            this.rexBuilder = relBuilder.getRexBuilder();
            this.outputFields = setOp.getRowType().getFieldList();
            this.inputs = setOp.getInputs();
            final int nInputs = inputs.size();
            this.diffSets = new ArrayList<>(nInputs);
            this.hasDiffs = new ArrayList<>(nInputs);
        }

        public static boolean equalRelDataType(RelDataType lhs, RelDataType rhs) {
            assert lhs != null && rhs != null;
            if (lhs.getSqlTypeName() != rhs.getSqlTypeName() || lhs.isNullable() != rhs.isNullable()) {
                return false;
            }
            RelDataType innerL = lhs.getComponentType();
            RelDataType innerR = rhs.getComponentType();
            if ((innerR != null && innerL != null)) {
                return equalRelDataType(innerL, innerR);
            }
            if ((innerR == null) != (innerL == null)) {
                return false;
            }
            if (lhs.isStruct() != rhs.isStruct()) {
                return false;
            }

            if (lhs.isStruct() && rhs.isStruct()) {
                List<RelDataTypeField> lFields = lhs.getFieldList();
                List<RelDataTypeField> rFields = rhs.getFieldList();
                if (lFields.size() != rFields.size()) {
                    return false;
                }
                for (int i = 0; i < lFields.size(); ++i) {
                    if (!equalRelDataType(lFields.get(i).getType(), rFields.get(i).getType())
                            || !(lFields.get(i).getName().equals(rFields.get(i).getName()))) {
                        return false;
                    }
                }
                return true;
            }
            return lhs.equals(rhs);
        }

        public boolean needConvert() {
            final int nOutputFields = outputFields.size();
            for (RelNode input : inputs) {
                final List<RelDataTypeField> inputFields = input.getRowType().getFieldList();
                assert inputFields.size() == nOutputFields;
                final BitSet diffRefs = new BitSet(nOutputFields);
                diffSets.add(diffRefs);
                for (int i = 0; i < nOutputFields; ++i) {
                    final RelDataType outputType = outputFields.get(i).getType();
                    final RelDataType inputType = inputFields.get(i).getType();
                    if (!equalRelDataType(inputType, outputType)) {
                        diffRefs.set(i);
                    }
                }
                final boolean hasDiff = IntStream.range(0, nOutputFields).anyMatch(diffRefs::get);
                hasDiffs.add(hasDiff);
            }
            return hasDiffs.stream().anyMatch(hasDiff -> hasDiff);
        }

        public RelNode convert() {
            final int nInputs = inputs.size();
            List<RelNode> newInputs = new ArrayList<>(nInputs);
            for (int i = 0; i < nInputs; ++i) {
                final RelNode input = inputs.get(i);
                if (hasDiffs.get(i)) {
                    newInputs.add(makeTypeCastInput(input, diffSets.get(i)));
                } else {
                    newInputs.add(input);
                }
            }
            return setOp.copy(setOp.getTraitSet(), newInputs);
        }

        private RelNode makeTypeCastInput(RelNode input, BitSet diffRefs) {
            final int nFields = input.getRowType().getFieldCount();
            final List<RexNode> exprs = new ArrayList<>(nFields);
            for (int i = 0; i < nFields; ++i) {
                final RexNode inputRef = rexBuilder.makeInputRef(input, i);
                if (diffRefs.get(i)) {
                    final RexNode castNode = rexBuilder.makeCast(
                            outputFields.get(i).getType(), inputRef, true);
                    exprs.add(castNode);
                } else {
                    exprs.add(inputRef);
                }
            }
            return relBuilder.push(input)
                    .projectNamed(exprs, input.getRowType().getFieldNames(), true)
                    .build();
        }
    }
}
