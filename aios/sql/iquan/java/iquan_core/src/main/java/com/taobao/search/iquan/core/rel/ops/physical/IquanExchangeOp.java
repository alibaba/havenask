package com.taobao.search.iquan.core.rel.ops.physical;

import com.taobao.search.iquan.core.api.config.IquanConfigManager;
import com.taobao.search.iquan.core.api.schema.Distribution;
import com.taobao.search.iquan.core.api.schema.Location;
import com.taobao.search.iquan.core.catalog.GlobalCatalog;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.plan.PlanWriteUtils;
import com.taobao.search.iquan.core.rel.programs.IquanOptContext;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import com.taobao.search.iquan.core.utils.RelDistributionUtil;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlExplainLevel;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class IquanExchangeOp extends Exchange implements IquanRelNode {
    private int parallelNum = -1;
    private int parallelIndex = -1;
    //output can be prunable(optional): 0(no), 1(yes), int type is friendly to GraphTransform
    private int outputPrunable = 0;

    public IquanExchangeOp(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelDistribution distribution) {
        super(cluster, traitSet, input, distribution);
    }

    @Override
    public Exchange copy(RelTraitSet traitSet, RelNode newInput, RelDistribution newDistribution) {
        IquanExchangeOp rel = new IquanExchangeOp(getCluster(), traitSet, newInput, newDistribution);
        rel.setParallelNum(getParallelNum());
        rel.setParallelIndex(getParallelIndex());
        return rel;
    }

    @Override
    public void explainInternal(final Map<String, Object> map, SqlExplainLevel level) {
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.DISTRIBUTION,
                PlanWriteUtils.formatShuffleDistribution(getDistribution(), input.getRowType()));

        if (level == SqlExplainLevel.ALL_ATTRIBUTES) {
            RelNode inputRelNode = getInput();
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.INPUT_FIELDS, PlanWriteUtils.formatRowFieldName(inputRelNode.getRowType()));
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.INPUT_FIELDS_TYPE, PlanWriteUtils.formatRowFieldType(inputRelNode.getRowType()));

            //RelMetadataQuery mq = this.getCluster().getMetadataQuery();
            //IquanMetadata.ScanNode mdScanNode = metadata(IquanMetadata.ScanNode.class, mq);
            //TableScan tableScan = mdScanNode.getScanNode();

            IquanConfigManager conf = IquanRelOptUtils.getConfigFromRel(this);

            if (inputRelNode instanceof TableScan) {
                PlanWriteUtils.formatTableInfo(map, (TableScan) inputRelNode, conf);
            } else {
                IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.CATALOG_NAME,
                        ((IquanOptContext)inputRelNode.getCluster().getPlanner().getContext()).getExecutor().getDefaultCatalogName());
                IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.DB_NAME,
                        ((IquanOptContext)inputRelNode.getCluster().getPlanner().getContext()).getExecutor().getDefaultDbName());
            }
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.TABLE_GROUP_NAME, ((IquanRelNode) inputRelNode).getLocation().getTableGroupName());
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.TABLE_DISTRIBUTION, RelDistributionUtil.formatDistribution((IquanRelNode) inputRelNode, true));
            map.put(ConstantDefine.OUTPUT_PRUNABLE, outputPrunable);

            //IquanMetadata.SubDAGPartitionPruning mdPruning = metadata(IquanMetadata.SubDAGPartitionPruning.class, mq);
            //IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.PARTITION_PRUNING, mdPruning.getPartitionPruning());
        }
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);

        final Map<String, Object> map = new TreeMap<>();
        SqlExplainLevel level = pw.getDetailLevel();

        IquanRelNode.explainIquanRelNode(this, map, level);
        explainInternal(map, level);

        pw.item(ConstantDefine.ATTRS, map);
        return pw;
    }

    @Override
    public RelNode accept(RexShuttle shuttle) {
        return super.accept(shuttle);
    }

    @Override
    public void acceptForTraverse(RexShuttle shuttle) {
        IquanRelNode.super.acceptForTraverse(shuttle);
    }

    @Override
    public String getName() {
        return "ExchangeOp";
    }

    @Override
    public Location getLocation() {
        return null;
    }

    @Override
    public void setLocation(Location location) {
        throw new RuntimeException("can not set location in IquanExchangeOp");
    }

    @Override
    public Distribution getOutputDistribution() {
        return (Distribution) getDistribution();
    }

    @Override
    public void setOutputDistribution(Distribution distribution) {
        throw new RuntimeException("can not set output distribution in IquanExchangeOp");
    }

    @Override
    public IquanRelNode deriveDistribution(List<RelNode> inputs, GlobalCatalog catalog, String dbName, IquanConfigManager config) {
        return null;
    }

    @Override
    public int getParallelNum() {
        return parallelNum;
    }

    @Override
    public void setParallelNum(int parallelNum) {
        this.parallelNum = parallelNum;
    }

    @Override
    public int getParallelIndex() {
        return parallelIndex;
    }

    @Override
    public void setParallelIndex(int parallelIndex) {
        this.parallelIndex = parallelIndex;
    }

    public void setOutputPrunable(int outputPrunable) {
        this.outputPrunable = outputPrunable;
    }
}
