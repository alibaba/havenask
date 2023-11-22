package com.taobao.search.iquan.core.utils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.config.IquanConfigManager;
import com.taobao.search.iquan.core.api.config.SqlConfigOptions;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.api.schema.Distribution;
import com.taobao.search.iquan.core.api.schema.HashType;
import com.taobao.search.iquan.core.api.schema.HashValues;
import com.taobao.search.iquan.core.api.schema.IndexType;
import com.taobao.search.iquan.core.api.schema.IquanTable;
import com.taobao.search.iquan.core.api.schema.Location;
import com.taobao.search.iquan.core.catalog.GlobalCatalog;
import com.taobao.search.iquan.core.catalog.function.internal.TableValueFunction;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.metadata.IquanMetadata;
import com.taobao.search.iquan.core.rel.ops.physical.IquanAggregateOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanExchangeOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanRelNode;
import com.taobao.search.iquan.core.rel.ops.physical.IquanSortOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableFunctionScanOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableScanBase;
import com.taobao.search.iquan.core.rel.ops.physical.Scope;
import com.taobao.search.iquan.core.rel.plan.PlanWriteUtils;
import com.taobao.search.iquan.core.rel.visitor.relvisitor.ExchangeVisitor;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlOperator;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelDistributionUtil {
    private static final Logger logger = LoggerFactory.getLogger(RelDistributionUtil.class);
    private static int MAX_STAGE = 3;

    public static Map<String, String> getPartFixKeyMap(List<String> tablePath, IquanTable iquanTable) {
        /*
        Map<String, String> partFixKeyMap = table.getPrimaryMap().keySet().stream()
                .map(PlanWriteUtils::unFormatFieldName)
                .collect(Collectors.toMap(pk -> pk, pk -> String.join(".", tablePath) + "." + pk));
        */
        Map<String, String> partFixKeyMap = new TreeMap<>();
        for (Map.Entry<String, IndexType> entry : iquanTable.getFieldToIndexMap().entrySet()) {
            switch (entry.getValue()) {
                case IT_PART_FIX:
                case IT_PRIMARY_KEY:
                case IT_PRIMARYKEY64:
                case IT_PRIMARYKEY128:
                    partFixKeyMap.put(entry.getKey(), String.join(".", tablePath) + "." + entry.getKey());
                    break;
                default:
                    break;
            }
        }
        return partFixKeyMap;
    }

    public static IquanRelNode checkIquanRelType(RelNode relNode) {
        if (!(relNode instanceof IquanRelNode)) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_DISTRIBUTION_RELNODE_INVALID,
                    String.format("not support rel node: %s", relNode));
        }
        return (IquanRelNode) relNode;
    }

    public static boolean existExchange(RelNode node) {
        ExchangeVisitor visitor = new ExchangeVisitor();
        visitor.go(node);
        return visitor.existExchange();
    }

    public static int getExchangeCount(RelNode node) {
        ExchangeVisitor visitor = new ExchangeVisitor(true);
        visitor.go(node);
        return visitor.getExchangeCount();
    }

    public static List<String> reorderHashFields(List<String> fields, List<Integer> hashFieldsPos) {
        /*
        if (hashFields.size() != hashFieldsPos.size()) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_DISTRIBUTION_REORDER_HASH_FIELDS_FAIL,
                    String.format("hash fields size %d is not equal to hash fields pos size %d", hashFields.size(), hashFieldsPos.size()));
        }
        */
        List<String> newHashFields = new ArrayList<>(hashFieldsPos.size());
        for (Integer pos : hashFieldsPos) {
            newHashFields.add(fields.get(pos));
        }
        return newHashFields;
    }

    /*
    public static boolean joinKeyContainDistribution(List<String> joinKeys, Distribution distribution) {
        if (distribution.getType() == RelDistribution.Type.SINGLETON
                || distribution.getType() == RelDistribution.Type.BROADCAST_DISTRIBUTED) {
            return true;
        } else if (distribution.getType() == RelDistribution.Type.RANDOM_DISTRIBUTED) {
            return false;
        } else if (distribution.getType() == RelDistribution.Type.HASH_DISTRIBUTED) {
            return joinKeys.containsAll(distribution.getHashFields());
        } else {
            return false;
        }
    }
    */

    public static Location getSingleLocationNode(GlobalCatalog catalog, IquanConfigManager config) {
        if (config.getBoolean(SqlConfigOptions.IQUAN_USE_DEFAULT_QRS)) {
            return Location.DEFAULT_QRS;
        }

        List<Location> qrsNodes = catalog.getLocationNodeManager().getQrsLocations();
        if (qrsNodes != null) {
            for (Location qrs : qrsNodes) {
                if (qrs.isSingle()) {
                    return qrs;
                }
            }
        }

        List<Location> computeNodes = catalog.getLocationNodeManager().getComputeNodeLocation();
        if (computeNodes != null) {
            for (Location candidateNode : computeNodes) {
                if (candidateNode.isSingle()) {
                    return candidateNode;
                }
            }
        }

        List<Location> searchers = catalog.getLocationNodeManager().getSearcherLocations();
        if (searchers != null) {
            for (Location searcher : searchers) {
                if (searcher.isSingle()) {
                    return searcher;
                }
            }
        }

        throw new SqlQueryException(IquanErrorCode.IQUAN_EC_DISTRIBUTION_NO_SINGLE_COMPUTE_NODE,
                String.format("has no single compute nodes in catalog : %s", catalog.getCatalogName()));
    }

    public static Location getNearComputeNode(int upLimitNum, GlobalCatalog catalog, IquanConfigManager config) {
        if (config.getBoolean(SqlConfigOptions.IQUAN_USE_DEFAULT_QRS)) {
            return Location.DEFAULT_QRS;
        }
        List<Location> computeNodes = catalog.getLocationNodeManager().getComputeNodeLocation();
        if ((computeNodes == null) || computeNodes.isEmpty()) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_DISTRIBUTION_EMPTY_COMPUTE_NODES,
                    String.format("has no compute node and default qrs node in catalog : %s", catalog.getCatalogName()));
        } else {
            // if db only has single compute node, targetNode must be single compute node
            Location targetNode = null;
            int minDiff = 0;
            for (Location candidateNode : computeNodes) {
                int curDiff = upLimitNum - candidateNode.getPartitionCnt();
                if (curDiff < 0) {
                    continue;
                }
                if (targetNode == null) {
                    targetNode = candidateNode;
                    minDiff = curDiff;
                    continue;
                }
                if (curDiff < minDiff) {
                    targetNode = candidateNode;
                    minDiff = curDiff;
                }
            }
            return targetNode;
        }
    }

    public static IquanAggregateOp genLocalAgg(IquanAggregateOp iquanAggregateOp,
                                               IquanRelNode input,
                                               List<AggregateCall> aggCalls,
                                               List<List<String>> aggCallAccNames,
                                               RelDataType partialAggRowType) {
        IquanAggregateOp localAgg = new IquanAggregateOp(
                iquanAggregateOp.getCluster(),
                iquanAggregateOp.getTraitSet(),
                iquanAggregateOp.getHints(),
                input,
                aggCalls,
                iquanAggregateOp.getGroupKeyFieldList(),
                iquanAggregateOp.getInputParams(),
                aggCallAccNames,
                partialAggRowType,
                Scope.PARTIAL
        );
        localAgg.setLocation(input.getLocation());
        localAgg.setOutputDistribution(input.getOutputDistribution());
        return localAgg;
    }

    public static IquanAggregateOp genGlobalAgg(IquanAggregateOp iquanAggregateOp,
                                                IquanRelNode input,
                                                List<AggregateCall> aggCalls,
                                                List<List<String>> aggCallAccNames,
                                                Location computeNode) {
        IquanExchangeOp exchangeOp;
        if ((computeNode.isSingle()) || (getExchangeCount(input) >= MAX_STAGE - 1)) {
            exchangeOp = new IquanExchangeOp(
                    iquanAggregateOp.getCluster(),
                    iquanAggregateOp.getTraitSet(),
                    input,
                    Distribution.SINGLETON
            );
        } else {
            Distribution distribution = Distribution.newBuilder(RelDistribution.Type.HASH_DISTRIBUTED, Distribution.EMPTY)
                    .partitionCnt(computeNode.getPartitionCnt())
                    .hashFunction(HashType.HF_HASH.getName())
                    .hashFields(iquanAggregateOp.getGroupKeyNames())
                    .build();
            exchangeOp = new IquanExchangeOp(
                    iquanAggregateOp.getCluster(),
                    iquanAggregateOp.getTraitSet(),
                    input,
                    distribution
            );
        }
        IquanAggregateOp finalAgg = new IquanAggregateOp(
                iquanAggregateOp.getCluster(),
                iquanAggregateOp.getTraitSet(),
                iquanAggregateOp.getHints(),
                exchangeOp,
                aggCalls,
                iquanAggregateOp.getGroupKeyFieldList(),
                aggCallAccNames,
                iquanAggregateOp.getOutputParams(),
                iquanAggregateOp.deriveRowType(),
                Scope.FINAL
        );
        finalAgg.setLocation(computeNode);
        finalAgg.setOutputDistribution(exchangeOp.getOutputDistribution());
        return finalAgg;
    }

    public static IquanSortOp genLocalSort(IquanSortOp iquanSortOp, IquanRelNode input) {
        RexBuilder rexBuilder = iquanSortOp.getCluster().getRexBuilder();
        int limit = iquanSortOp.getLimit();
        int offset = iquanSortOp.getOffset();
        int num = limit + offset;
        num = num > 0 ? num : Integer.MAX_VALUE;
        IquanSortOp localSortOp = new IquanSortOp(
                iquanSortOp.getCluster(),
                iquanSortOp.getTraitSet(),
                input,
                iquanSortOp.getCollation(),
                rexBuilder.makeBigintLiteral(new BigDecimal(0)),
                rexBuilder.makeBigintLiteral(new BigDecimal(num))
        );
        localSortOp.setOutputDistribution(input.getOutputDistribution());
        localSortOp.setLocation(input.getLocation());
        return localSortOp;
    }

    public static IquanRelNode genGlobalSort(IquanRelNode input, IquanSortOp output, GlobalCatalog catalog, IquanConfigManager config) {
        if (input.isSingle() || input.isBroadcast()) {
            output.replaceInput(0, input);
            return output.simpleRelDerive(input);
        }
        IquanExchangeOp exchangeOp = new IquanExchangeOp(
                output.getCluster(),
                output.getTraitSet(),
                input,
                Distribution.SINGLETON
        );
        output.replaceInput(0, exchangeOp);
        Location computeNode = RelDistributionUtil.getSingleLocationNode(catalog, config);
        output.setOutputDistribution(Distribution.SINGLETON);
        output.setLocation(computeNode);
        return output;
    }

    public static boolean isTvfNormalScope(IquanTableFunctionScanOp functionScan) {
        boolean isNormalScope = true;
        SqlOperator operator = ((RexCall) functionScan.getCall()).getOperator();
        if (!(operator instanceof TableValueFunction)) {
            return isNormalScope;
        }
        TableValueFunction tableValueFunction = (TableValueFunction) operator;
        Map<String, Object> properties = tableValueFunction.getProperties();

        Object normalScopeParam = properties.get(ConstantDefine.NORMAL_SCOPE);
        if (normalScopeParam != null) {
            if (normalScopeParam instanceof Boolean) {
                isNormalScope = (Boolean) normalScopeParam;
            } else if (normalScopeParam instanceof String) {
                isNormalScope = Boolean.parseBoolean((String) normalScopeParam);
            }
        }

        if (!isNormalScope) {
            if (!tableValueFunction.isIdentityFields()) {
                logger.warn("table value function {} has not identity output fields, force set to normal_score",
                        tableValueFunction.getTvfFunction().getName());
                isNormalScope = true;
            } else if (tableValueFunction.getDistribution().getPartitionCnt() == 1) {
                logger.warn("partition cnt of table value function {} is 1, force set to normal_score",
                        tableValueFunction.getTvfFunction().getName());
                isNormalScope = true;
            }
        }
        return isNormalScope;
    }

    public static void getTableFieldExpr(RelNode relNode, Map<String, Object> outputFieldExprMap, boolean judgeFuncProperty) {
        while (!(relNode instanceof TableScan)) {
            if (relNode instanceof Calc) {
                RexProgram rexProgram = ((Calc) relNode).getProgram();
                outputFieldExprMap.putAll(PlanWriteUtils.innerFormatOutputRowExpr(rexProgram, judgeFuncProperty));
            }
            List<RelNode> inputs = relNode.getInputs();
            if (inputs.size() != 1) {
                break;
            }
            relNode = IquanRelOptUtils.toRel(inputs.get(0));
        }
        if (relNode instanceof IquanTableScanBase) {
            List<IquanRelNode> pushDownOps = ((IquanTableScanBase) relNode).getPushDownOps();
            for (IquanRelNode iquanRelNode : pushDownOps) {
                if (iquanRelNode instanceof Calc) {
                    RexProgram rexProgram = ((Calc) iquanRelNode).getProgram();
                    outputFieldExprMap.putAll(PlanWriteUtils.innerFormatOutputRowExpr(rexProgram, judgeFuncProperty));
                }
            }
        }
    }

    public static TableScan getBottomScan(RelNode relNode) {
        while (!(relNode instanceof TableScan)) {
            List<RelNode> inputs = relNode.getInputs();
            if (inputs.size() != 1) {
                break;
            }
            if (inputs.get(0) instanceof Exchange) {
                break;
            }
            relNode = IquanRelOptUtils.toRel(inputs.get(0));
        }
        if (relNode instanceof TableScan) {
            return (TableScan) relNode;
        } else {
            return null;
        }
    }

    public static RelOptTable getBottomTable(RelNode relNode) {
        TableScan tableScan = getBottomScan(relNode);
        if (tableScan != null) {
            return tableScan.getTable();
        }
        return null;
    }

    public static int getFieldIndexInRow(RelDataType relDataType, String fieldName) {
        List<RelDataTypeField> fieldList = relDataType.getFieldList();
        for (int i = 0; i < fieldList.size(); i++) {
            if (fieldList.get(i).getName().equals(fieldName)) {
                return i;
            }
        }
        return -1;
    }

    public static Map<String, Object> formatDistribution(IquanRelNode iquanRelNode, boolean needHashValues) {
        Map<String, Object> tableDistribution = new TreeMap<>();
        Distribution distribution = iquanRelNode.getOutputDistribution();
        if (distribution == null) {
            return null;
        }
        tableDistribution.put(ConstantDefine.TYPE, distribution.getType().name());
        tableDistribution.put(ConstantDefine.PARTITION_CNT, distribution.getPartitionCnt());
        IquanRelOptUtils.addMapIfNotEmpty(tableDistribution, ConstantDefine.EQUAL_HASH_FIELDS, distribution.getEqualHashFields());
        IquanRelOptUtils.addMapIfNotEmpty(tableDistribution, ConstantDefine.PART_FIX_FIELDS, distribution.getPartFixKeyMap());
        IquanRelOptUtils.addMapIfNotEmpty(tableDistribution, ConstantDefine.HASH_MODE, PlanWriteUtils.formatTableHashMode(distribution));
        if (needHashValues) {
            RelMetadataQuery mq = iquanRelNode.getCluster().getMetadataQuery();
            IquanMetadata.ScanNode mdScanNode = iquanRelNode.metadata(IquanMetadata.ScanNode.class, mq);
            List<TableScan> scanList = mdScanNode.getScanNodes();
            formatHashValues(tableDistribution, scanList);
        }
        return tableDistribution;
    }

    private static void formatHashValues(Map<String, Object> attrMap, List<TableScan> scanList) {
        Map<IquanTable, HashValues> tableHashValuesMap = new HashMap<>();
        for (TableScan tableScan : scanList) {
            if (!(tableScan instanceof IquanTableScanBase)) {
                continue;
            }
            IquanTable iquanTable = IquanRelOptUtils.getIquanTable(tableScan.getTable());
            RexProgram rexProgram = ((IquanTableScanBase) tableScan).getNearestRexProgram();
            PlanWriteUtils.getTableScanHashValues(iquanTable.getDistribution(), (IquanTableScanBase) tableScan, rexProgram);
            HashValues hashValues = ((IquanTableScanBase) tableScan).getHashValues();
            if (hashValues.isEmpty()) {
                continue;
            }
            if (iquanTable.getDistribution().getPartitionCnt() == 1) {
                continue;
            }
            if (tableHashValuesMap.containsKey(iquanTable)) {
                if (!tableHashValuesMap.get(iquanTable).merge(iquanTable.getDistribution().getPosHashFields(), hashValues)) {
                    throw new SqlQueryException(IquanErrorCode.IQUAN_EC_PLAN_HASH_VALUES_MERGE_ERROR, "can not reach here");
                }
            } else {
                tableHashValuesMap.put(iquanTable, hashValues.copy());
            }
        }
        if (tableHashValuesMap.size() >= 1) {
            //if size > 1, maybe join or multi table union, only output one
            Map.Entry<IquanTable, HashValues> entry = tableHashValuesMap.entrySet().iterator().next();
            IquanTable iquanTable = entry.getKey();
            HashValues hashValues = entry.getValue();
            String partitionIds = hashValues.getAssignedPartitionIds();
            if (StringUtils.isNotBlank(partitionIds)) {
                IquanRelOptUtils.addMapIfNotEmpty(attrMap, ConstantDefine.ASSIGNED_PARTITION_IDS, partitionIds);
            } else {
                PlanWriteUtils.outputTableScanHashValues(attrMap, hashValues);
                if (!hashValues.getHashValuesMap().isEmpty() || StringUtils.isNotEmpty(hashValues.getAssignedHashValues())) {
                    IquanRelOptUtils.addMapIfNotEmpty(attrMap, ConstantDefine.HASH_MODE, PlanWriteUtils.formatTableHashMode(iquanTable.getDistribution()));
                }
            }
        }
    }
}
