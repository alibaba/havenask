package com.taobao.search.iquan.core.rel.ops.physical;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.config.IquanConfigManager;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.api.schema.Distribution;
import com.taobao.search.iquan.core.api.schema.Location;
import com.taobao.search.iquan.core.catalog.GlobalCatalog;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.plan.PlanWriteUtils;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import com.taobao.search.iquan.core.utils.RelDistributionUtil;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.commons.lang3.tuple.Triple;

public class IquanSortOp extends Sort implements IquanRelNode {
    private int parallelNum = -1;
    private int parallelIndex = -1;
    private Location location;
    private Distribution distribution;

    public IquanSortOp(RelOptCluster cluster, RelTraitSet traits, RelNode child, RelCollation collation, RexNode offset, RexNode fetch) {
        super(cluster, traits, child, collation, offset, fetch);
    }

    @Override
    public Sort copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, RexNode offset, RexNode fetch) {
        IquanSortOp rel = new IquanSortOp(getCluster(), traitSet, newInput, newCollation, offset, fetch);
        rel.setParallelNum(parallelNum);
        rel.setParallelIndex(parallelIndex);
        rel.setOutputDistribution(distribution);
        rel.setLocation(location);
        return rel;
    }

    public int getLimit() {
        if (this.fetch != null) {
            if (this.fetch instanceof RexDynamicParam) {
                return -1;
            } else {
                return RexLiteral.intValue(this.fetch);
            }
        } else {
            return Integer.MAX_VALUE;
        }
    }

    public int getOffset() {
        if (this.offset != null) {
            if (this.offset instanceof RexDynamicParam) {
                return -1;
            } else {
                return RexLiteral.intValue(this.offset);
            }
        } else {
            return 0;
        }
    }

    public boolean hasCollation() {
        if (collation != null && collation.getFieldCollations().size() > 0) {
            return true;
        }
        return false;
    }

    @Override
    public void explainInternal(final Map<String, Object> map, SqlExplainLevel level) {
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.LIMIT, getLimit());
        PlanWriteUtils.formatCollations(map, collation, input.getRowType());

        if (level == SqlExplainLevel.ALL_ATTRIBUTES) {
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.OFFSET, getOffset());
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
        if (hasCollation()) {
            return "SortOp";
        } else {
            return "LimitOp";
        }
    }

    @Override
    public Location getLocation() {
        return location;
    }

    @Override
    public void setLocation(Location location) {
        this.location = location;
    }

    @Override
    public Distribution getOutputDistribution() {
        return distribution;
    }

    @Override
    public void setOutputDistribution(Distribution distribution) {
        this.distribution = distribution;
    }

    private IquanSortOp genLocalSort(IquanSortOp iquanSortOp) {
        if (hasCollation() && iquanSortOp.hasCollation()) {
            if (collation.equals(iquanSortOp.getCollation())) {
                int limit = Math.min(getLimit(), iquanSortOp.getLimit());
                return (IquanSortOp) iquanSortOp.copy(
                        iquanSortOp.getTraitSet(),
                        iquanSortOp.getInput(),
                        iquanSortOp.getCollation(),
                        iquanSortOp.offset,
                        iquanSortOp.getCluster().getRexBuilder().makeBigintLiteral(new BigDecimal(limit))
                );
            } else {
                return RelDistributionUtil.genLocalSort(this, iquanSortOp);
            }
        } else if (hasCollation()) {
            //sort(input) data is not decided
            return RelDistributionUtil.genLocalSort(this, iquanSortOp);
        } else if (iquanSortOp.hasCollation()) {
            if (getOffset() > 0) {
                return iquanSortOp;
            } else {
                int limit = Math.min(getLimit(), iquanSortOp.getLimit());
                return (IquanSortOp) iquanSortOp.copy(
                        iquanSortOp.getTraitSet(),
                        iquanSortOp.getInput(),
                        iquanSortOp.getCollation(),
                        iquanSortOp.offset,
                        iquanSortOp.getCluster().getRexBuilder().makeBigintLiteral(new BigDecimal(limit))
                );
            }
        } else {
            int limit = Math.min(getLimit(), iquanSortOp.getLimit());
            return (IquanSortOp) iquanSortOp.copy(
                    iquanSortOp.getTraitSet(),
                    iquanSortOp.getInput(),
                    iquanSortOp.getCollation(),
                    iquanSortOp.offset,
                    iquanSortOp.getCluster().getRexBuilder().makeBigintLiteral(new BigDecimal(limit))
            );
        }
    }

    @Override
    public IquanRelNode derivePendingUnion(IquanUnionOp pendingUnion, GlobalCatalog catalog, IquanConfigManager config) {
        Location finalUnionLocation = RelDistributionUtil.getSingleLocationNode(catalog, config);
        List<Triple<Location, Distribution, List<RelNode>>> locationList = pendingUnion.getLocationList();
        List<RelNode> newInputs = new ArrayList<>(locationList.size());
        for (Triple<Location, Distribution, List<RelNode>> triple : locationList) {
            IquanSortOp localSortOp;
            if (triple.getRight().size() == 1) {
                IquanRelNode iquanRelNode = (IquanRelNode) triple.getRight().get(0);
                if (iquanRelNode instanceof IquanSortOp) {
                    localSortOp = genLocalSort((IquanSortOp) iquanRelNode);
                } else {
                    localSortOp = RelDistributionUtil.genLocalSort(this, iquanRelNode);
                }
            } else {
                IquanUnionOp sameLocationUnion = new IquanUnionOp(pendingUnion.getCluster(), pendingUnion.getTraitSet(), triple.getRight(), pendingUnion.all, null);
                sameLocationUnion.setLocation(triple.getLeft());
                sameLocationUnion.setOutputDistribution(triple.getMiddle());
                localSortOp = RelDistributionUtil.genLocalSort(this, sameLocationUnion);
            }
            if (finalUnionLocation.equals(triple.getLeft())) {
                newInputs.add(localSortOp);
            } else {
                newInputs.add(localSortOp.singleExchange());
            }
        }
        IquanUnionOp finalUnion = new IquanUnionOp(
                pendingUnion.getCluster(),
                pendingUnion.getTraitSet(),
                newInputs,
                pendingUnion.all,
                null
        );
        finalUnion.setOutputDistribution(Distribution.SINGLETON);
        finalUnion.setLocation(finalUnionLocation);
        replaceInput(0, finalUnion);
        return simpleRelDerive(finalUnion);
    }

    @Override
    public IquanRelNode deriveDistribution(List<RelNode> inputs, GlobalCatalog catalog, IquanConfigManager config) {
        IquanRelNode iquanRelNode = RelDistributionUtil.checkIquanRelType(inputs.get(0));
        if (isPendingUnion(iquanRelNode)) {
            return derivePendingUnion((IquanUnionOp) iquanRelNode, catalog, config);
        }
        Distribution inputDistribution = iquanRelNode.getOutputDistribution();
        RelDistribution.Type inputDistributionType = inputDistribution.getType();
        boolean limitPushDown = false;
        if (!hasCollation()) {
            limitPushDown = limitPushDown(iquanRelNode);
        }
        if (inputDistributionType == RelDistribution.Type.SINGLETON
                || inputDistributionType == RelDistribution.Type.BROADCAST_DISTRIBUTED) {
            if (limitPushDown && getOffset() == 0) {
                return iquanRelNode;
            } else {
                return simpleRelDerive(iquanRelNode);
            }
        } else {
            IquanRelNode newInput;
            if (limitPushDown) {
                newInput = iquanRelNode;
            } else {
                newInput = RelDistributionUtil.genLocalSort(this, iquanRelNode);
            }
            return RelDistributionUtil.genGlobalSort(newInput, this, catalog, config);
        }
    }

    private boolean limitPushDown(IquanRelNode iquanRelNode) {
        IquanTableScanBase iquanScanBase;
        if (iquanRelNode instanceof IquanTableScanBase) {
            iquanScanBase = (IquanTableScanBase) iquanRelNode;
        } else if (iquanRelNode instanceof IquanIdentityOp) {
            IquanRelNode inputRelNode = (IquanRelNode) ((IquanIdentityOp) iquanRelNode).getInput();
            if (inputRelNode instanceof IquanTableScanBase) {
                iquanScanBase = (IquanTableScanBase) inputRelNode;
            } else {
                return false;
            }
        } else {
            return false;
        }
        int limit = getLimit();
        int offset = getOffset();
        if (limit < 0 || offset < 0) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_ORDER_UNSUPPORT_DYNAMIC_PARAM, "");
        }
        int num = (limit + offset > 0) ? (limit + offset) : Integer.MAX_VALUE;
        int newLimit = Math.min(num, iquanScanBase.getLimit());
        iquanScanBase.setLimit(newLimit);
        return true;
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
}
