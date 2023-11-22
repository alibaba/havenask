package com.taobao.search.iquan.core.rel.metadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.collect.ImmutableList;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.config.IquanConfigManager;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.metadata.IquanMetadata.SubDAGPartitionPruning;
import com.taobao.search.iquan.core.rel.ops.physical.ExecLookupJoinOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanCorrelateOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanJoinOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanMergeOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableFunctionScanOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableScanBase;
import com.taobao.search.iquan.core.rel.ops.physical.IquanUnionOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanValuesOp;
import com.taobao.search.iquan.core.rel.plan.PlanWriteUtils;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

/**
 * Default implementations of the
 * {@link IquanMetadata.SubDAGPartitionPruning}
 * metadata provider for Physical RelNode.
 */
public class IquanRelMDSubDAGPartitionPruning implements MetadataHandler<SubDAGPartitionPruning> {
    public static final RelMetadataProvider SOURCE =
            ReflectiveRelMetadataProvider.reflectiveSource(new IquanRelMDSubDAGPartitionPruning(), IquanMetadata.SubDAGPartitionPruning.METHOD);

    public IquanRelMDSubDAGPartitionPruning() {
    }

    @Override
    public MetadataDef<SubDAGPartitionPruning> getDef() {
        return IquanMetadata.SubDAGPartitionPruning.DEF;
    }

    public Object getPartitionPruning(TableScan r, RelMetadataQuery mq) {
        return null;
    }

    public Object getPartitionPruning(IquanTableScanBase r, RelMetadataQuery mq) {
        IquanConfigManager conf = IquanRelOptUtils.getConfigFromRel(r);
        final Map<String, Object> map = new TreeMap<>();
        PlanWriteUtils.formatTableInfo(map, r, conf);
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.TABLE_DISTRIBUTION,
                PlanWriteUtils.formatScanDistribution(r));
        return map;
    }

    public Object getPartitionPruning(IquanValuesOp r, RelMetadataQuery mq) {
        return null;
    }

    public Object getPartitionPruning(HepRelVertex r, RelMetadataQuery mq) {
        RelNode input = r.getCurrentRel();
        return getPruningInfos(input, mq);
    }

    public Object getPartitionPruning(IquanUnionOp r, RelMetadataQuery mq) {
        List<RelNode> inputs = r.getInputs();
        return getPruningInfos(inputs, ConstantDefine.OR, mq);
    }

    public Object getPartitionPruning(IquanMergeOp r, RelMetadataQuery mq) {
        List<RelNode> inputs = r.getInputs();
        return getPruningInfos(inputs, ConstantDefine.OR, mq);
    }

    public Object getPartitionPruning(IquanJoinOp r, RelMetadataQuery mq) {
        if (r.getJoinType() == JoinRelType.SEMI) {
            return getPruningInfos(r.getLeft(), r.getRight(), ConstantDefine.AND, mq);
        } else if (r.getJoinType() == JoinRelType.ANTI) {
            return getPruningInfos(r.getLeft(), mq);
        } else if (r.getJoinType() == JoinRelType.LEFT) {
            return getPruningInfos(r.getLeft(), mq);
        } else if (r.getJoinType() == JoinRelType.RIGHT) {
            return getPruningInfos(r.getRight(), mq);
        } else if (r.getJoinType() == JoinRelType.INNER) {
            return getPruningInfos(r.getLeft(), r.getRight(), ConstantDefine.AND, mq);
        }
        return null;
    }

    public Object getPartitionPruning(IquanTableFunctionScanOp r, RelMetadataQuery mq) {
        List<RelNode> inputs = r.getInputs();
        return getPruningInfos(inputs, ConstantDefine.OR, mq);
    }

    public Object getPartitionPruning(IquanCorrelateOp r, RelMetadataQuery mq) {
        return getPruningInfos(r.getLeft(), mq);
    }

    public Object getPartitionPruning(ExecLookupJoinOp r, RelMetadataQuery mq) {
        return getPruningInfos(r.getJoinOp(), mq);
    }

    public Object getPartitionPruning(RelNode r, RelMetadataQuery mq) {
        List<RelNode> inputs = r.getInputs();
        if (inputs.size() > 1) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_FAIL, "IquanRelMDSubDAGPartitionPruning: can not process this rel node: " + r.getDigest());
        }
        return getPruningInfos(inputs.get(0), mq);
    }

    private Object getPruningInfos(RelNode input, RelMetadataQuery mq) {
        IquanMetadata.SubDAGPartitionPruning metadata = input.metadata(IquanMetadata.SubDAGPartitionPruning.class, mq);
        return metadata.getPartitionPruning();
    }

    private Object getPruningInfos(RelNode left, RelNode right, String operator, RelMetadataQuery mq) {
        ImmutableList<RelNode> inputs = ImmutableList.of(left, right);
        return getPruningInfos(inputs, operator, mq);
    }

    private Object getPruningInfos(List<RelNode> inputs, String operator, RelMetadataQuery mq) {
        IquanMetadata.SubDAGPartitionPruning metadata;
        List<Object> values = new ArrayList<>();

        for (RelNode input : inputs) {
            metadata = input.metadata(IquanMetadata.SubDAGPartitionPruning.class, mq);
            Object value = metadata.getPartitionPruning();
            if (value != null) {
                values.add(value);
            }
        }

        if (values.isEmpty()) {
            return null;
        } else if (values.size() == 1) {
            return values.get(0);
        }

        Map<String, Object> attrMap = new TreeMap<>();
        attrMap.put(ConstantDefine.OP, operator);
        attrMap.put(ConstantDefine.PARAMS, values);
        return attrMap;
    }
}
