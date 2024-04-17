package com.taobao.search.iquan.core.rel.ops.physical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.config.IquanConfigManager;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.api.schema.Distribution;
import com.taobao.search.iquan.core.api.schema.HashValues;
import com.taobao.search.iquan.core.api.schema.IquanTable;
import com.taobao.search.iquan.core.api.schema.Location;
import com.taobao.search.iquan.core.catalog.GlobalCatalog;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.plan.PlanWriteUtils;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import com.taobao.search.iquan.core.utils.RelDistributionUtil;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexVariable;
import org.apache.calcite.runtime.FlatLists;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.commons.lang3.tuple.MutableTriple;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

public interface IquanRelNode extends RelNode {
    String getName();

    default Location getLocation() {
        return null;
    }

    default void setLocation(Location location) {
    }

    default Distribution getOutputDistribution() {
        return null;
    }

    default void setOutputDistribution(Distribution distribution) {
    }

    default IquanRelNode deriveDistribution(List<RelNode> inputs, GlobalCatalog catalog, IquanConfigManager config) {
        return null;
    }

    default boolean isSingle() {
        //return 1 == getLocation().getPartitionCnt();
        return getOutputDistribution().getType() == RelDistribution.Type.SINGLETON;
    }

    default boolean isBroadcast() {
        return getOutputDistribution().getType() == RelDistribution.Type.BROADCAST_DISTRIBUTED;
    }

    default int getParallelNum() {
        return -1;
    }

    default void setParallelNum(int parallelNum) {
    }

    default int getParallelIndex() {
        return -1;
    }

    default void setParallelIndex(int parallelIndex) {
    }

    /**
     * only for collect desired information
     *
     * @param shuttle
     */
    default void acceptForTraverse(RexShuttle shuttle) {
    }

    default void explainInternal(final Map<String, Object> map, SqlExplainLevel level) {
    }

    static void explainIquanRelNode(RelNode relNode, final Map<String, Object> map, SqlExplainLevel level) {
        if (!(relNode instanceof IquanRelNode)) {
            return;
        }

        IquanRelNode iquanRelNode = (IquanRelNode) relNode;

        int parallelNum = iquanRelNode.getParallelNum();
        int parallelIndex = iquanRelNode.getParallelIndex();

        if (parallelNum > 1 && parallelIndex >= 0) {
            map.put(ConstantDefine.PARALLEL_NUM, parallelNum);
            map.put(ConstantDefine.PARALLEL_INDEX, parallelIndex);
        }

        if (level == SqlExplainLevel.ALL_ATTRIBUTES) {
            RelDataType dataType = relNode.getRowType();
            long hashKey = PlanWriteUtils.formatRelDataTypeHash(dataType);
            map.put(ConstantDefine.OUTPUT_FIELDS_HASH, hashKey);
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.OUTPUT_DISTRIBUTION, RelDistributionUtil.formatDistribution(iquanRelNode, false));
            Location location = iquanRelNode.getLocation();
            if (location != null) {
                Map<String, Object> locationMeta = new TreeMap<>();
                locationMeta.put(ConstantDefine.NODE_NAME, location.getNodeName());
                locationMeta.put("location_table_name", location.getLocationTableName());
                locationMeta.put(ConstantDefine.PARTITION_CNT, location.getPartitionCnt());
                map.put(ConstantDefine.LOCATION, locationMeta);
            }
        }
    }

    default List<String> getInputNames() {
        return ImmutableList.of(ConstantDefine.INPUT);
    }

    default IquanRelNode simpleRelDerive(IquanRelNode input) {
        setLocation(input.getLocation());
        setOutputDistribution(input.getOutputDistribution());
        return this;
    }

    default IquanExchangeOp singleExchange() {
        return new IquanExchangeOp(
                getCluster(),
                getTraitSet(),
                this,
                Distribution.SINGLETON
        );
    }

    default IquanExchangeOp broadcastExchange() {
        return new IquanExchangeOp(
                getCluster(),
                getTraitSet(),
                this,
                Distribution.BROADCAST
        );
    }

    default IquanExchangeOp rangeExchange(List<String> hashFields, Distribution srcDistribution) {
        Distribution distribution = Distribution.RANGE;
        distribution.setHashFields(hashFields);
        distribution.setHashFunction(srcDistribution.getHashFunction().getName());
        distribution.setHashParams(srcDistribution.getHashParams());
        distribution.setPartitionCnt(srcDistribution.getPartitionCnt());
        return new IquanExchangeOp(
                getCluster(),
                getTraitSet(),
                this,
                distribution
        );
    }

    default void shuffleToDataNode(boolean prunable, IquanRelNode sourceNode, List<String> sourceJoinKeys, int sourceOrdinal,
                                   Distribution targetDistribution, Location targetLocation, List<Integer> targetHashFieldsPos) {
        List<String> newSourceHashFields;
        if (targetHashFieldsPos == null) {
            newSourceHashFields = sourceJoinKeys;
        } else {
            newSourceHashFields = RelDistributionUtil.reorderHashFields(sourceJoinKeys, targetHashFieldsPos);
        }
        if (targetHashFieldsPos != null) {
            IquanExchangeOp sourceExchange;
            if (targetHashFieldsPos.size() == targetDistribution.getPosHashFields().size()) {
                Distribution newSourceDistribution = Distribution.newBuilder(targetDistribution.getType(), Distribution.EMPTY)
                        .partitionCnt(targetDistribution.getPartitionCnt())
                        .hashFunction(targetDistribution.getHashFunction().getName())
                        .hashFields(newSourceHashFields)
                        .hashParams(targetDistribution.getHashParams())
                        .build();
                sourceExchange = new IquanExchangeOp(
                        sourceNode.getCluster(),
                        sourceNode.getTraitSet(),
                        sourceNode,
                        newSourceDistribution
                );
            } else {
                //ROUTING_HASH but join keys only has the first hash field
                sourceExchange = sourceNode.rangeExchange(newSourceHashFields, targetDistribution);
            }
            if (prunable) {
                sourceExchange.setOutputPrunable(1);
            }
            replaceInput(sourceOrdinal, sourceExchange);
        } else {
            replaceInput(sourceOrdinal, sourceNode.broadcastExchange());
        }
        setOutputDistribution(targetDistribution);
        setLocation(targetLocation);
    }

    default boolean isPendingUnion(IquanRelNode iquanRelNode) {
        if (iquanRelNode instanceof IquanUnionOp) {
            Distribution inputDistribution = iquanRelNode.getOutputDistribution();
            if (inputDistribution == null) {
                return true;
            }
        }
        return false;
    }

    default Calc mergeCalc(IquanCalcOp topCalc, IquanCalcOp bottomCalc) {
        topCalc.setOutputDistribution(bottomCalc.getOutputDistribution());
        topCalc.setLocation(bottomCalc.getLocation());
        final RexProgram topProgram = topCalc.getProgram();
        final List<RexNode> exprList = new ArrayList<>(topProgram.getExprList());
        final List<RexLocalRef> projectList = new ArrayList<>(topProgram.getProjectList());
        final List<RelDataTypeField> fields = bottomCalc.getRowType().getFieldList();
        final RelOptCluster cluster = topCalc.getCluster();
        for (int i = 0; i < exprList.size(); i++) {
            if (exprList.get(i) instanceof RexInputRef) {
                RexInputRef exprRef = (RexInputRef) exprList.get(i);
                RelDataType srcType = fields.get(exprRef.getIndex()).getType();
                if (!srcType.equals(exprRef.getType())) {
                    exprList.set(i, cluster.getRexBuilder().makeInputRef(srcType, exprRef.getIndex()));
                }
            }
        }
        for (int i = 0; i < projectList.size(); i++) {
            RexLocalRef projectRef = projectList.get(i);
            if (exprList.get(projectRef.getIndex()) instanceof RexInputRef) {
                RelDataType srcType = fields.get(projectRef.getIndex()).getType();
                if (!srcType.equals(projectRef.getType())) {
                    projectList.set(i, new RexLocalRef(projectRef.getIndex(), srcType));
                }
            }
        }
        RelDataType outputRowType = cluster.getTypeFactory().createStructType(
                projectList.stream().map(RexVariable::getType).collect(Collectors.toList()),
                topProgram.getOutputRowType().getFieldNames());
        topProgram.getOutputRowType().getFieldNames();
        RexProgram newTopProgram = new RexProgram(
                bottomCalc.getRowType(),
                exprList,
                projectList,
                topProgram.getCondition(),
                outputRowType);
        RexProgram mergedProgram = RexProgramBuilder.mergePrograms(
                newTopProgram,
                bottomCalc.getProgram(),
                topCalc.getCluster().getRexBuilder());
        return topCalc.copy(
                topCalc.getTraitSet(),
                bottomCalc.getInput(),
                mergedProgram);
    }

    default IquanRelNode derivePendingUnion(IquanUnionOp pendingUnion, GlobalCatalog globalCatalog, IquanConfigManager config) {
        List<Triple<Location, Distribution, List<RelNode>>> locationList = pendingUnion.getLocationList();
        List<RelNode> newInputs = new ArrayList<>(locationList.size());
        IquanRelNode iquanRelNode;
        List<RelNode> inputs;
        for (Triple<Location, Distribution, List<RelNode>> triple : locationList) {
            if (triple.getRight().size() == 1) {
                inputs = triple.getRight();
                if ((this instanceof IquanCalcOp) && inputs.get(0) instanceof IquanCalcOp) {
                    final IquanCalcOp topCalc = (IquanCalcOp) this;
                    final IquanCalcOp bottomCalc = (IquanCalcOp) inputs.get(0);
                    newInputs.add(mergeCalc(topCalc, bottomCalc));
                    continue;
                }

            } else {
                IquanRelNode sameLocationUnion = new IquanUnionOp(pendingUnion.getCluster(), pendingUnion.getTraitSet(), triple.getRight(), pendingUnion.all, null);
                sameLocationUnion.setOutputDistribution(triple.getMiddle());
                sameLocationUnion.setLocation(triple.getLeft());
                inputs = Collections.singletonList(sameLocationUnion);
            }
            if (this instanceof IquanJoinOp) {
                IquanJoinOp iquanJoinOp = (IquanJoinOp) this;
                if (iquanJoinOp.getLeft() == pendingUnion) {
                    inputs = FlatLists.of(inputs.get(0), iquanJoinOp.getRight());
                } else {
                    inputs = FlatLists.of(iquanJoinOp.getLeft(), inputs.get(0));
                }
            }
            iquanRelNode = (IquanRelNode) this.copy(getTraitSet(), inputs);
            newInputs.add(iquanRelNode.deriveDistribution(inputs, globalCatalog, config));
        }
        List<Triple<Location, Distribution, List<RelNode>>> outputLocationList = new ArrayList<>(locationList.size());
        IquanUnionOp finalUnion = new IquanUnionOp(
                pendingUnion.getCluster(),
                pendingUnion.getTraitSet(),
                newInputs,
                pendingUnion.all,
                outputLocationList
        );
        return finalUnion.deriveDistribution(newInputs, globalCatalog, config);
    }

    default int getLocationIndex(IquanRelNode iquanRelNode,
                                 Pair<IquanTable, HashValues> tableAndHashValues,
                                 List<Triple<Location, Distribution, List<RelNode>>> locationList,
                                 List<String> locationPartitionIdsList,
                                 List<Map<IquanTable, HashValues>> locationTableHashValuesList) {
        String curAssignedPartitionIds = null;
        IquanTable curIquanTable = null;
        if (tableAndHashValues != null) {
            curAssignedPartitionIds = tableAndHashValues.getRight().getAssignedPartitionIds();
            curIquanTable = tableAndHashValues.getLeft();
        }
        TreeMap<Integer, Integer> priorityIndexMap = new TreeMap<>();
        int priority1 = 1, priority2 = 2, priority3 = 3, priority4 = 4;
        for (int index = 0; index < locationList.size(); index++) {
            Triple<Location, Distribution, List<RelNode>> triple = locationList.get(index);
            Map<IquanTable, HashValues> tableHashValuesMap = locationTableHashValuesList.get(index);
            String partitionIds = locationPartitionIdsList.get(index);
            if (!triple.getLeft().equals(iquanRelNode.getLocation())) {
                continue;
            }
            if ((partitionIds != null) && partitionIds.equals(curAssignedPartitionIds)) {
                priorityIndexMap.put(priority1, index);
                continue;
            }
            if ((curIquanTable != null) && tableHashValuesMap.containsKey(curIquanTable)) {
                priorityIndexMap.put(priority2, index);
                continue;
            }
            if (triple.getMiddle().getPartitionCnt() == iquanRelNode.getOutputDistribution().getPartitionCnt()) {
                priorityIndexMap.put(priority3, index);
                continue;
            }
            priorityIndexMap.put(priority4, index);
        }
        if (priorityIndexMap.isEmpty()) {
            return -1;
        }
        return priorityIndexMap.firstEntry().getValue();
    }

    default int analyzeInputs(List<Triple<Location, Distribution, List<RelNode>>> locationList, List<RelNode> inputs) {
        locationList.clear();
        List<Map<IquanTable, HashValues>> locationTableHashValuesList = new ArrayList<>();
        List<String> locationPartitionIdsList = new ArrayList<>();
        List<RelNode> pendingUnionInputs = new ArrayList<>();
        for (RelNode input : inputs) {
            IquanRelNode iquanRelNode = (IquanRelNode) input;
            Location location = iquanRelNode.getLocation();
            if (location == null) {
                if (iquanRelNode instanceof IquanUnionOp) {
                    //iquanRelNode is pendingUnion
                    pendingUnionInputs.addAll(iquanRelNode.getInputs());
                } else {
                    throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_UNION_INPUT_ERROR, "can not reach here");
                }
            } else {
                processInput(iquanRelNode, locationList, locationPartitionIdsList, locationTableHashValuesList);
            }
        }
        if (pendingUnionInputs.size() > 0) {
            for (RelNode input : pendingUnionInputs) {
                processInput((IquanRelNode) input, locationList, locationPartitionIdsList, locationTableHashValuesList);
            }
        }
        return pendingUnionInputs.size();
    }

    default void processInput(IquanRelNode iquanRelNode,
                              List<Triple<Location, Distribution, List<RelNode>>> locationList,
                              List<String> locationPartitionIdsList,
                              List<Map<IquanTable, HashValues>> locationTableHashValuesList) {
        Pair<IquanTable, HashValues> tableAndHashValues = getTableAndHashValues(iquanRelNode);
        int index = getLocationIndex(iquanRelNode, tableAndHashValues,
                locationList, locationPartitionIdsList, locationTableHashValuesList);
        if (index == -1) {
            addNewLocationTriple(iquanRelNode, tableAndHashValues, locationList, locationPartitionIdsList, locationTableHashValuesList);
        } else {
            MutableTriple<Location, Distribution, List<RelNode>> oldLocationTriple =
                    (MutableTriple<Location, Distribution, List<RelNode>>) locationList.get(index);
            Distribution comDistribution = new Distribution.Builder(RelDistribution.Type.ANY, Distribution.EMPTY).build();
            String partitionIds = locationPartitionIdsList.get(index);
            Map<IquanTable, HashValues> tableHashValuesMap = locationTableHashValuesList.get(index);
            if (canShareOld(iquanRelNode, tableAndHashValues, oldLocationTriple.middle, comDistribution, partitionIds, tableHashValuesMap)) {
                oldLocationTriple.middle = comDistribution;
                oldLocationTriple.right.add(iquanRelNode);
            } else {
                addNewLocationTriple(iquanRelNode, tableAndHashValues, locationList, locationPartitionIdsList, locationTableHashValuesList);
            }
        }
    }

    default void addNewLocationTriple(IquanRelNode iquanRelNode,
                                      Pair<IquanTable, HashValues> tableAndHashValues,
                                      List<Triple<Location, Distribution, List<RelNode>>> locationList,
                                      List<String> locationPartitionIdsList,
                                      List<Map<IquanTable, HashValues>> locationTableHashValuesList) {
        List<RelNode> inputList = new ArrayList<>();
        inputList.add(iquanRelNode);
        locationList.add(MutableTriple.of(iquanRelNode.getLocation(), iquanRelNode.getOutputDistribution(), inputList));
        String partitionIds = null;
        Map<IquanTable, HashValues> tableHashValuesMap = new HashMap<>();
        if ((tableAndHashValues != null)) {
            HashValues hashValues = tableAndHashValues.getRight();
            tableHashValuesMap.put(tableAndHashValues.getLeft(), hashValues.copy());
            partitionIds = hashValues.getAssignedPartitionIds();
        }
        locationPartitionIdsList.add(partitionIds);
        locationTableHashValuesList.add(tableHashValuesMap);
    }

    default Pair<IquanTable, HashValues> getTableAndHashValues(IquanRelNode iquanRelNode) {
        TableScan tableScan = RelDistributionUtil.getBottomScan(iquanRelNode);
        if (!(tableScan instanceof IquanTableScanBase)) {
            return null;
        }
        IquanTableScanBase scanBase = (IquanTableScanBase) tableScan;
        IquanTable iquanTable = IquanRelOptUtils.getIquanTable(scanBase.getTable());
        RexProgram rexProgram = scanBase.getNearestRexProgram();
        PlanWriteUtils.getTableScanHashValues(iquanTable.getDistribution(), scanBase, rexProgram);
        return Pair.of(iquanTable, scanBase.getHashValues());
    }

    default boolean canShareOld(IquanRelNode iquanRelNode, Pair<IquanTable, HashValues> tableAndHashValues,
                                Distribution oldDistribution, Distribution comDistribution,
                                String partitionIds, Map<IquanTable, HashValues> tableHashValuesMap) {
        if (!canMergeHashValues(tableAndHashValues, partitionIds, tableHashValuesMap)) {
            return false;
        }
        Distribution newDistribution = iquanRelNode.getOutputDistribution();
        boolean partitionCntSame = (oldDistribution.getPartitionCnt() == newDistribution.getPartitionCnt());
        if (partitionCntSame) {
            comDistribution.setPartitionCnt(oldDistribution.getPartitionCnt());
        } else {
            return false;
        }

        if ((oldDistribution.getType() == RelDistribution.Type.SINGLETON) && (newDistribution.getType() == RelDistribution.Type.SINGLETON)) {
            comDistribution.setType(RelDistribution.Type.SINGLETON);
            comDistribution.setPartitionCnt(1);
            return true;
        }

        boolean distributionTypeSame = (oldDistribution.getType() == newDistribution.getType());
        boolean hashFunctionSame = (oldDistribution.getHashFunction() == newDistribution.getHashFunction());
        if (hashFunctionSame) {
            comDistribution.setHashFunction(oldDistribution.getHashFunction().getName());
        }
        boolean hashFieldsSame = oldDistribution.getPosHashFields().equals(newDistribution.getPosHashFields());
        if (hashFieldsSame) {
            comDistribution.setPosHashFields(oldDistribution.getPosHashFields());
        }
        boolean hashParamsSame = oldDistribution.getHashParams().equals(newDistribution.getHashParams());
        if (hashParamsSame) {
            comDistribution.setHashParams(oldDistribution.getHashParams());
        }
        if (!distributionTypeSame || !hashFunctionSame || !hashFieldsSame || !hashParamsSame) {
            comDistribution.setType(RelDistribution.Type.RANDOM_DISTRIBUTED);
        } else {
            comDistribution.setType(oldDistribution.getType());
            comDistribution.setHashFunction(oldDistribution.getHashFunction().getName());
            comDistribution.setPosHashFields(oldDistribution.getPosHashFields());
            comDistribution.setHashParams(oldDistribution.getHashParams());
            comDistribution.setPartitionCnt(oldDistribution.getPartitionCnt());
            if (!newDistribution.getPartFixKeyMap().isEmpty()
                    && oldDistribution.getPartFixKeyMap().values().containsAll(newDistribution.getPartFixKeyMap().values())) {
                //same table union, parent op not add exchange when use partition fixed join key
                comDistribution.setEqualHashFields(oldDistribution.getEqualHashFields());
                comDistribution.setPartFixKeyMap(oldDistribution.getPartFixKeyMap());
            }
        }
        return true;
    }

    default boolean canMergeHashValues(Pair<IquanTable, HashValues> tableAndHashValues, String partitionIds, Map<IquanTable, HashValues> tableHashValuesMap) {
        if ((tableAndHashValues == null) || tableHashValuesMap.isEmpty()) {
            return true;
        }
        boolean merged = false;
        HashValues tableHashValues = tableHashValuesMap.get(tableAndHashValues.getLeft());
        HashValues curHashValues = tableAndHashValues.getRight();
        if ((partitionIds != null) && partitionIds.equals(curHashValues.getAssignedPartitionIds())) {
            //process assigned partition ids first
            merged = true;
        } else if (tableHashValues != null) {
            //merge same table HashValues second
            if (tableHashValues.merge(tableAndHashValues.getLeft().getDistribution().getPosHashFields(), curHashValues)) {
                merged = true;
            }
        } else {
            //process new table last
            if (curHashValues.isEmpty()) {
                for (HashValues hashValues : tableHashValuesMap.values()) {
                    if (hashValues.isEmpty()) {
                        merged = true;
                        break;
                    }
                }
            }
        }
        return merged;
    }
}
