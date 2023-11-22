package com.taobao.search.iquan.core.rel.ops.physical.explain;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.taobao.search.iquan.core.api.schema.FieldMeta;
import com.taobao.search.iquan.core.api.schema.IquanTable;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.hint.IquanHintCategory;
import com.taobao.search.iquan.core.rel.hint.IquanHintOptUtils;
import com.taobao.search.iquan.core.rel.ops.physical.IquanJoinOp;
import com.taobao.search.iquan.core.rel.plan.PlanWriteUtils;
import com.taobao.search.iquan.core.utils.IquanJoinUtils;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.ImmutableIntList;

public class IquanJoinExplain {
    protected IquanJoinOp join;

    public IquanJoinExplain(IquanJoinOp join) {
        this.join = join;
    }

    private void explainJoinInputAttrs(Map<String, Object> map, SqlExplainLevel level) {
        if (level == SqlExplainLevel.ALL_ATTRIBUTES) {
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.LEFT_INPUT_FIELDS,
                    PlanWriteUtils.formatRowFieldName(join.getLeft().getRowType()));
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.RIGHT_INPUT_FIELDS,
                    PlanWriteUtils.formatRowFieldName(join.getRight().getRowType()));
        }
    }

    protected void explainJoinInternalAttrs(Map<String, Object> map, SqlExplainLevel level) {
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.OUTPUT_FIELDS,
                PlanWriteUtils.formatRowFieldName(join.getRowType()));

        RelDataType internalRowType = join.getInternalRowType();
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.CONDITION,
                PlanWriteUtils.formatExprImpl(join.getCondition(), null, internalRowType));

        map.put(ConstantDefine.JOIN_TYPE, join.getJoinType().name());
        map.put(ConstantDefine.SEMI_JOIN_TYPE, join.getJoinType().name());

        JoinInfo joinInfo = join.analyzeCondition();
        RexNode equiCondition = joinInfo.getEquiCondition(join.getLeft(), join.getRight(),
                join.getCluster().getRexBuilder());
        RexNode remainingCondition = joinInfo.getRemaining(join.getCluster().getRexBuilder());

        map.put(ConstantDefine.IS_EQUI_JOIN, IquanJoinUtils.isEquiJoin(join));
        if (!equiCondition.isAlwaysTrue()) {
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.EQUI_CONDITION,
                    PlanWriteUtils.formatExprImpl(equiCondition, null, internalRowType));
        }
        if (!remainingCondition.isAlwaysTrue()) {
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.REMAINING_CONDITION,
                    PlanWriteUtils.formatExprImpl(remainingCondition, null, internalRowType));
        }

        if (level == SqlExplainLevel.ALL_ATTRIBUTES) {
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.OUTPUT_FIELDS_INTERNAL,
                    PlanWriteUtils.formatRowFieldName(internalRowType));
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.OUTPUT_FIELDS_TYPE,
                    PlanWriteUtils.formatRowFieldType(join.getRowType()));
            map.put(ConstantDefine.JOIN_SYSTEM_FIELD_NUM, join.getSystemFieldList().size());

            RelNode leftInput = IquanRelOptUtils.toRel(join.getLeft());
            IquanTable leftIquanTable = IquanRelOptUtils.getIquanTable(join.getLeft());
            Set<String> leftKeys = calcJoinKeys(leftInput, joinInfo.leftKeys);
            if (leftIquanTable != null && !leftKeys.isEmpty()) {
                List<FieldMeta> fieldMetaList = leftIquanTable.getFieldMetaList();
                IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.LEFT_TABLE_META,
                        PlanWriteUtils.formatTableMeta(fieldMetaList, leftKeys));
            }

            RelNode rightInput = IquanRelOptUtils.toRel(join.getRight());
            IquanTable rightIquanTable = IquanRelOptUtils.getIquanTable(join.getRight());
            Set<String> rightKeys = calcJoinKeys(rightInput, joinInfo.rightKeys);
            if (rightIquanTable != null && !rightKeys.isEmpty()) {
                List<FieldMeta> fieldMetaList = rightIquanTable.getFieldMetaList();
                IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.RIGHT_TABLE_META, PlanWriteUtils
                        .formatTableMeta(fieldMetaList, rightKeys));
            }

            // dump hints
            Map<String, Object> hintMap = new TreeMap<>();
            {
                RelHint hint = IquanHintOptUtils.resolveHints(join, IquanHintCategory.CAT_JOIN_ATTR);
                if (hint != null) {
                    IquanRelOptUtils.addMapIfNotEmpty(hintMap, hint.hintName, hint.kvOptions);
                }
            }
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.HINTS_ATTR, hintMap);
        }
    }

    private Set<String> calcJoinKeys(RelNode node, ImmutableIntList keys) {
        Set<String> keyStrs = new HashSet<>();
        List<RelDataTypeField> fields = node.getRowType().getFieldList();
        for (int i = 0; i < keys.size(); ++i) {
            keyStrs.add(PlanWriteUtils.formatFieldName(fields.get(keys.get(i)).getName()));
        }
        return keyStrs;
    }

    public void explainJoinAttrs(Map<String, Object> map, SqlExplainLevel level) {
        explainJoinInternalAttrs(map, level);
        explainJoinInputAttrs(map, level);
    }
}
