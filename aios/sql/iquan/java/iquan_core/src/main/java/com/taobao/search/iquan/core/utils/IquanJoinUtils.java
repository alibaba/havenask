package com.taobao.search.iquan.core.utils;

import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.api.schema.Distribution;
import com.taobao.search.iquan.core.api.schema.IndexType;
import com.taobao.search.iquan.core.api.schema.Table;
import com.taobao.search.iquan.core.rel.ops.physical.IquanExchangeOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanJoinOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanRelNode;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableScanBase;
import com.taobao.search.iquan.core.rel.plan.PlanWriteUtils;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

public class IquanJoinUtils {

    public static boolean isEquiJoin(Join join) {
        JoinInfo joinInfo = join.analyzeCondition();
        RelNode leftInput = IquanRelOptUtils.toRel(join.getLeft());
        RelNode rightInput = IquanRelOptUtils.toRel(join.getRight());
        RexNode equiCondition = joinInfo.getEquiCondition(leftInput, rightInput, join.getCluster().getRexBuilder());
        return !(equiCondition instanceof RexLiteral);
    }

    public static boolean isScannable(final Table table, final RelNode input) {
        if (null == table || table.getTableType().isScannable()) {
            return true;
        }
        if (input instanceof IquanTableScanBase && !((IquanTableScanBase) input).isSimple()) {
            return true;
        }

        RexProgram rexProgram;
        if (input instanceof IquanTableScanBase) {
            rexProgram = ((IquanTableScanBase) input).getNearestRexProgram();
        } else {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_JOIN_INVALID, "input is not scan op");
        }

        if (table.getTableType().isFilterable() && rexProgram != null && rexProgram.getCondition() != null) {
            return true;
        }
        return false;
    }

    public static List<String> calcJoinKeyNames(RelNode relNode, ImmutableIntList keys, boolean format) {
        List<String> keyNames = new ArrayList<>();
        List<RelDataTypeField> fields = relNode.getRowType().getFieldList();
        for (Integer key : keys) {
            String name = fields.get(key).getName();
            name = format ? PlanWriteUtils.formatFieldName(name) : name;
            keyNames.add(name);
        }
        return keyNames;
    }

    public static Map<String, String> calcExactJoinKeyNames(RelNode relNode, ImmutableIntList keys, boolean format) {
        //an insertion-ordered map
        Map<String, String> keyNameMap = new LinkedHashMap<>();
        Map<String, Object> outputFieldExprMap = new HashMap<>();
        RelDistributionUtil.getTableFieldExpr(relNode, outputFieldExprMap);
        List<RelDataTypeField> fields = relNode.getRowType().getFieldList();
        String mapKey;
        String mapValue;
        for (Integer key : keys) {
            String name = fields.get(key).getName();
            mapKey = format ? PlanWriteUtils.formatFieldName(name) : name;
            Object exprValue = outputFieldExprMap.get(PlanWriteUtils.formatFieldName(name));
            if (exprValue == null) {
                mapValue = mapKey;
            } else if (exprValue instanceof String) {
                mapValue = format ? (String) exprValue : PlanWriteUtils.unFormatFieldName((String) exprValue);
            } else {
                mapValue = exprValue.toString();
            }
            keyNameMap.put(mapKey, mapValue);
        }
        return keyNameMap;
    }

    public static boolean hasIndex(final Table table, List<String> keys) {
        Map<String, IndexType> fieldToIndexMap = table.getFieldToIndexMap();
        return !keys.isEmpty() && keys.stream().map(fieldToIndexMap::get).anyMatch(
                v -> v != null && v != IndexType.IT_NONE);
    }

    public static List<Integer> calcHashFieldsPos(Distribution distribution, List<String> keys) {
        if (distribution.getType() != RelDistribution.Type.HASH_DISTRIBUTED) {
            return null;
        }
        List<List<String>> equalHashFields = distribution.getEqualHashFields();
        List<MutablePair<Integer, String>> posHashFields = distribution.getPosHashFields();
        if (posHashFields.isEmpty()) {
            return null;
        }
        List<Integer> indexList = new ArrayList<>(posHashFields.size());
        int index;
        boolean preMatched = false;
        for (int i = 0; i < posHashFields.size(); i++) {
            String hashField = posHashFields.get(i).right;
            index = keys.indexOf(hashField);
            if ((index < 0) && (posHashFields.get(i).left < equalHashFields.size())) {
                for (String equalHashField : equalHashFields.get(posHashFields.get(i).left)) {
                    index = keys.indexOf(equalHashField);
                    if (index >= 0)
                        break;
                }
            }
            if (index < 0) {
                if (preMatched) {
                    return indexList;
                } else {
                    return null;
                }
            }
            preMatched = true;
            indexList.add(index);
        }
        return indexList;
    }

    public static boolean samePartFixKeyJoin(List<String> probeJoinKeys, Map<String, String> probePartFixKeyMap,
                                             List<String> buildJoinKeys, Map<String, String> buildPartFixKeyMap) {
        String probeJoinKey, buildJoinKey;
        String probeOriginUniqueKey, buildOriginUniqueKey;
        for (int i = 0; i < probeJoinKeys.size(); i++) {
            probeJoinKey = probeJoinKeys.get(i);
            buildJoinKey = buildJoinKeys.get(i);
            probeOriginUniqueKey = probePartFixKeyMap.get(probeJoinKey);
            buildOriginUniqueKey = buildPartFixKeyMap.get(buildJoinKey);
            if (probeOriginUniqueKey == null || buildOriginUniqueKey == null) {
                continue;
            }
            if (StringUtils.equals(probePartFixKeyMap.get(probeJoinKey), buildPartFixKeyMap.get(buildJoinKey))) {
                return true;
            }
        }
        return false;
    }

    private static void addRenamedFixKey(IquanJoinOp iquanJoinOp,
                                         List<RelDataTypeField> outputFields, List<RelDataTypeField> checkFields,
                                         Map<String, String> srcPartFixKeyMap, Map<String, String> destPartFixKeyMap) {
        int offset = iquanJoinOp.getLeft().getRowType().getFieldCount();
        for (Map.Entry<String, String> entry : srcPartFixKeyMap.entrySet()) {
            for (int i = 0; i < checkFields.size(); i++) {
                if (entry.getKey().equals(checkFields.get(i).getName())) {
                    destPartFixKeyMap.put(outputFields.get(offset + i).getName(), entry.getValue());
                }
            }
        }
    }

    private static void updatePartFixKey(IquanJoinOp iquanJoinOp, Map<String, String> partFixKeyMap,
                                         IquanRelNode probeNode, List<String> probeJoinKeys,
                                         IquanRelNode buildNode, List<String> buildJoinKeys) {
        partFixKeyMap.clear();
        String probeJoinKey, buildJoinKey;
        String probeOriginUniqueKey, buildOriginUniqueKey;
        boolean probePartFixKeyJoin = false, buildPartFixKeyJoin = false;
        List<RelDataTypeField> checkFields = iquanJoinOp.getRight().getRowType().getFieldList();
        List<RelDataTypeField> outputFieldList = iquanJoinOp.getRowType().getFieldList();
        Map<String, String> probePartFixKeyMap = probeNode.getOutputDistribution().getPartFixKeyMap();
        Map<String, String> buildPartFixKeyMap = buildNode.getOutputDistribution().getPartFixKeyMap();
        RelDistribution.Type probeDistributionType = probeNode.getOutputDistribution().getType();
        RelDistribution.Type buildDistributionType = buildNode.getOutputDistribution().getType();
        //distribution must be neither expand nor shrink
        boolean saveProbe = (probeDistributionType == RelDistribution.Type.HASH_DISTRIBUTED) || (probeDistributionType == RelDistribution.Type.RANDOM_DISTRIBUTED);
        boolean saveBuild = (buildDistributionType == RelDistribution.Type.HASH_DISTRIBUTED) || (buildDistributionType == RelDistribution.Type.RANDOM_DISTRIBUTED);
        if (probeNode instanceof IquanExchangeOp) {
            IquanRelNode inputNode = (IquanRelNode) ((IquanExchangeOp) probeNode).getInput();
            probePartFixKeyMap = inputNode.getOutputDistribution().getPartFixKeyMap();
        }
        if (buildNode instanceof IquanExchangeOp) {
            IquanRelNode inputNode = (IquanRelNode) ((IquanExchangeOp) buildNode).getInput();
            buildPartFixKeyMap = inputNode.getOutputDistribution().getPartFixKeyMap();
        }
        if (iquanJoinOp.isSemiJoin()) {
            if (iquanJoinOp.isLeftIsBuild()) {
                partFixKeyMap.putAll(buildPartFixKeyMap);
            } else {
                partFixKeyMap.putAll(probePartFixKeyMap);
            }
            return;
        }
        for (int i = 0; i < probeJoinKeys.size(); i++) {
            probeJoinKey = probeJoinKeys.get(i);
            buildJoinKey = buildJoinKeys.get(i);
            probeOriginUniqueKey = probePartFixKeyMap.get(probeJoinKey);
            buildOriginUniqueKey = buildPartFixKeyMap.get(buildJoinKey);
            probePartFixKeyJoin = probePartFixKeyJoin || (probeOriginUniqueKey != null);
            buildPartFixKeyJoin = buildPartFixKeyJoin || (buildOriginUniqueKey != null);
        }
        if (probePartFixKeyJoin || saveBuild) {
            if (iquanJoinOp.isLeftIsBuild()) {
                partFixKeyMap.putAll(buildPartFixKeyMap);
            } else {
                addRenamedFixKey(iquanJoinOp, outputFieldList, checkFields, buildPartFixKeyMap, partFixKeyMap);
            }
        }
        if (buildPartFixKeyJoin || saveProbe) {
            if (iquanJoinOp.isLeftIsBuild()) {
                addRenamedFixKey(iquanJoinOp, outputFieldList, checkFields, probePartFixKeyMap, partFixKeyMap);
            } else {
                partFixKeyMap.putAll(probePartFixKeyMap);
            }
        }
    }

    public static void updateDistribution(boolean samePartFixKeyJoin, boolean hashFieldInRight,
                                          IquanJoinOp iquanJoinOp, Distribution outputDistribution,
                                          IquanRelNode probeNode, List<String> probeJoinKeys,
                                          IquanRelNode buildNode, List<String> buildJoinKeys) {
        updatePartFixKey(iquanJoinOp, outputDistribution.getPartFixKeyMap(),
                probeNode, probeJoinKeys,buildNode, buildJoinKeys);
        if (outputDistribution.getType() != RelDistribution.Type.HASH_DISTRIBUTED) {
            return;
        }
        List<MutablePair<Integer, String>> posHashFields = outputDistribution.getPosHashFields();
        List<RelDataTypeField> outputFieldList = iquanJoinOp.getRowType().getFieldList();
        List<RelDataTypeField> rightSrcFields = iquanJoinOp.getRight().getRowType().getFieldList();
        int leftFieldCount = iquanJoinOp.getLeft().getRowType().getFieldCount();
        if (hashFieldInRight) {
            for (MutablePair<Integer, String> posHashField : posHashFields) {
                for (int j = 0; j < rightSrcFields.size(); j++) {
                    if (posHashField.right.equals(rightSrcFields.get(j).getName())) {
                        if (iquanJoinOp.isSemiJoin()) {
                            int key = 0;
                            JoinInfo joinInfo = iquanJoinOp.analyzeCondition();
                            ImmutableIntList rightKeys = joinInfo.rightKeys;
                            while((rightKeys.get(key) != j) && (++key < rightKeys.size()));
                            String fieldName = outputFieldList.get(joinInfo.leftKeys.get(key)).getName();
                            if(!posHashField.right.equals(fieldName)) {
                                posHashField.right = fieldName;
                            }
                        } else if (!posHashField.right.equals(outputFieldList.get(leftFieldCount + j).getName())) {
                            posHashField.right = outputFieldList.get(leftFieldCount + j).getName();
                        }
                        break;
                    }
                }
            }
        }
        IquanRelNode leftRelNode = probeNode;
        IquanRelNode rightRelNode = buildNode;
        if (iquanJoinOp.isLeftIsBuild()) {
            leftRelNode = buildNode;
            rightRelNode = probeNode;
        }
        if (iquanJoinOp.isSemiJoin()) {
            if (leftRelNode instanceof IquanExchangeOp || leftRelNode.isBroadcast()) {
                //only use hashFields
                outputDistribution.getEqualHashFields().clear();
            } else {
                //use left equalHashFields
                outputDistribution.setEqualHashFields(leftRelNode.getOutputDistribution().getEqualHashFields());
            }
        } else {
            List<List<String>> equalHashFieldList = getEqualHashFieldList(hashFieldInRight, leftFieldCount, iquanJoinOp, posHashFields);
            boolean needMergeLeft = !(leftRelNode instanceof IquanExchangeOp || leftRelNode.isBroadcast());
            boolean needMergeRight = !(rightRelNode instanceof IquanExchangeOp || rightRelNode.isBroadcast());
            if (needMergeLeft) {
                List<List<String>> addLists = getAddLists(samePartFixKeyJoin, hashFieldInRight, posHashFields.size(), leftRelNode);
                equalHashFieldList = mergeEqualHashFieldList(false, leftFieldCount,
                        iquanJoinOp, equalHashFieldList, addLists);
            }
            if (needMergeRight) {
                List<List<String>> addLists = getAddLists(samePartFixKeyJoin, !hashFieldInRight, posHashFields.size(), rightRelNode);
                equalHashFieldList = mergeEqualHashFieldList(true, leftFieldCount,
                        iquanJoinOp, equalHashFieldList, addLists);
            }
            outputDistribution.setEqualHashFields(equalHashFieldList);
        }
    }

    private static List<List<String>> getAddLists(boolean samePartFixKeyJoin, boolean hashFieldUntreated,
                                                  int size, IquanRelNode iquanRelNode) {
        List<List<String>> addLists = iquanRelNode.getOutputDistribution().getEqualHashFields();
        if (samePartFixKeyJoin && addLists.isEmpty() && hashFieldUntreated) {
            List<MutablePair<Integer, String>> tmpPosHashFields = iquanRelNode.getOutputDistribution().getPosHashFields();
            if (tmpPosHashFields.size() == size) {
                addLists = ListUtils.partition(iquanRelNode.getOutputDistribution().getHashFields(), 1);
            } else if (tmpPosHashFields.size() < size) {
                addLists = new ArrayList<>(size);
                for (int i = 0 ;i < tmpPosHashFields.size(); i++) {
                    Pair<Integer, String> tmpPosHashField = tmpPosHashFields.get(i);
                    while (addLists.size() < tmpPosHashField.getLeft()) {
                        addLists.add(new ArrayList<>());
                    }
                    addLists.add(tmpPosHashField.getLeft(), Collections.singletonList(tmpPosHashField.getRight()));
                }
            } else {
                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_JOIN_INVALID, "join input [" + iquanRelNode.getDigest() + "] hash fields error");
            }
        }
        return addLists;
    }

    private static List<List<String>> getEqualHashFieldList(boolean hashFieldInRight, int leftFieldCount,
                                                            IquanJoinOp iquanJoinOp, List<MutablePair<Integer, String>> posHashFields) {
        List<List<String>> equalHashFieldList = new ArrayList<>(posHashFields.size());
        for (Pair<Integer, String> posHashField : posHashFields) {
            equalHashFieldList.add(new ArrayList<>(2));
            equalHashFieldList.get(posHashField.getLeft()).add(posHashField.getRight());
        }
        List<RelDataTypeField> fieldList = iquanJoinOp.getRowType().getFieldList();
        JoinInfo joinInfo = iquanJoinOp.analyzeCondition();
        int size = joinInfo.leftKeys.size();
        for (int i = 0; i < size; i++) {
            int leftKey = joinInfo.leftKeys.get(i);
            int rightKey = joinInfo.rightKeys.get(i);
            int offset = rightKey + leftFieldCount;
            String checkJoinFieldName = fieldList.get(leftKey).getName();
            int index = -1;
            if (hashFieldInRight) {
                offset = leftKey;
                checkJoinFieldName = fieldList.get(rightKey + leftFieldCount).getName();
            }
            for (Pair<Integer, String> posHashField : posHashFields) {
                if (posHashField.getRight().equals(checkJoinFieldName)) {
                    index = posHashField.getLeft();
                    break;
                }
            }
            if(index >= 0) {
                equalHashFieldList.get(index).add(fieldList.get(offset).getName());
            }
        }
        return equalHashFieldList;
    }

    private static List<List<String>> mergeEqualHashFieldList(boolean mergeRight, int leftFieldCount, IquanJoinOp iquanJoinOp,
                                                              List<List<String>> baseLists, List<List<String>> addLists) {
        if (addLists.isEmpty()) {
            return baseLists;
        }
        List<List<String>> equalHashFieldList = new ArrayList<>(baseLists.size());
        List<RelDataTypeField> outputFieldList = iquanJoinOp.getRowType().getFieldList();
        int a = 0; int index; String field;
        for (; a < baseLists.size() && a < addLists.size(); a++) {
            List<String> basePartFields = baseLists.get(a);
            List<String> addPartFields = addLists.get(a);
            List<String> newPartFields = new ArrayList<>(basePartFields.size() + addPartFields.size());
            newPartFields.addAll(basePartFields);
            for (String addField : addPartFields) {
                if (mergeRight) {
                    index = RelDistributionUtil.getFieldIndexInRow(iquanJoinOp.getRight().getRowType(), addField);
                    field = outputFieldList.get(leftFieldCount + index).getName();
                } else {
                    index = RelDistributionUtil.getFieldIndexInRow(iquanJoinOp.getLeft().getRowType(), addField);
                    field = outputFieldList.get(index).getName();
                }
                if (!newPartFields.contains(field)) newPartFields.add(field);
            }
            equalHashFieldList.add(newPartFields);
        }
        for (; a < baseLists.size(); a++) {
            equalHashFieldList.add(baseLists.get(a));
        }
        return equalHashFieldList;
    }
}
