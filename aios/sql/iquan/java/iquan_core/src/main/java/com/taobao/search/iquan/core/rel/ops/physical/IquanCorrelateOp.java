package com.taobao.search.iquan.core.rel.ops.physical;

import com.taobao.search.iquan.core.api.config.IquanConfigManager;
import com.taobao.search.iquan.core.api.schema.Distribution;
import com.taobao.search.iquan.core.api.schema.Location;
import com.taobao.search.iquan.core.catalog.GlobalCatalog;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import com.taobao.search.iquan.core.utils.RelDistributionUtil;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A relational operator that performs nested-loop joins.
 *
 * <p>It behaves like a kind of {@link org.apache.calcite.rel.core.Join},
 * but works by setting variables in its environment and restarting its
 * right-hand input.
 *
 * <p>A Correlate is used to represent a correlated query. One
 * implementation strategy is to de-correlate the expression.
 *
 * <table>
 * <caption>Mapping of physical operations to logical ones</caption>
 * <tr><th>Physical operation</th><th>Logical operation</th></tr>
 * <tr><td>NestedLoops</td><td>Correlate(A, B, regular)</td></tr>
 * <tr><td>NestedLoopsOuter</td><td>Correlate(A, B, outer)</td></tr>
 * <tr><td>NestedLoopsSemi</td><td>Correlate(A, B, semi)</td></tr>
 * <tr><td>NestedLoopsAnti</td><td>Correlate(A, B, anti)</td></tr>
 * <tr><td>HashJoin</td><td>EquiJoin(A, B)</td></tr>
 * <tr><td>HashJoinOuter</td><td>EquiJoin(A, B, outer)</td></tr>
 * <tr><td>HashJoinSemi</td><td>SemiJoin(A, B, semi)</td></tr>
 * <tr><td>HashJoinAnti</td><td>SemiJoin(A, B, anti)</td></tr>
 * </table>
 */
public class IquanCorrelateOp extends Correlate implements IquanRelNode {
    private int parallelNum = -1;
    private int parallelIndex = -1;
    private Location location;
    private Distribution distribution;

    public IquanCorrelateOp(RelOptCluster cluster,
                            RelTraitSet traitSet,
                            RelNode left,
                            RelNode right,
                            CorrelationId correlationId,
                            ImmutableBitSet requiredColumns,
                            JoinRelType joinType) {
        super(cluster, traitSet, left, right, correlationId, requiredColumns, joinType);
        assert !CalciteSystemProperty.DEBUG.value() || isValid(Litmus.THROW, null);
    }

    @Override
    public Correlate copy(RelTraitSet traitSet,
                          RelNode left, RelNode right, CorrelationId correlationId,
                          ImmutableBitSet requiredColumns, JoinRelType joinType) {
        IquanCorrelateOp rel = new IquanCorrelateOp(getCluster(), traitSet, left, right,
                correlationId, requiredColumns, joinType);
        rel.setParallelNum(parallelNum);
        rel.setParallelIndex(parallelIndex);
        return rel;
    }

    @Override
    public void explainInternal(final Map<String, Object> map, SqlExplainLevel level) {
        map.put(ConstantDefine.JOIN_TYPE, joinType.name());
        map.put(ConstantDefine.CORRELATE_ID, correlationId.getId());
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.REQUIRED_COLUMNS, requiredColumns.toList());
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        // Note: can't call super.explainTerms(pw), copy code from BiRel.explainTerms()
        pw.input("left", left);
        pw.input("right", right);

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
        return "CorrelateOp";
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

    @Override
    public IquanRelNode deriveDistribution(List<RelNode> inputs, GlobalCatalog catalog, String dbName, IquanConfigManager config) {
        IquanRelNode pendingNode = null;
        IquanRelNode determinedNode = null;
        for (RelNode relNode : inputs) {
            IquanRelNode iquanRelNode = RelDistributionUtil.checkIquanRelType(relNode);
            Location location = iquanRelNode.getLocation();
            if (location == null || location.equals(Location.UNKNOWN)) {
                assert pendingNode == null;
                pendingNode = iquanRelNode;
            } else if (determinedNode == null) {
                determinedNode = iquanRelNode;
            } else {
                assert determinedNode.getLocation().equals(iquanRelNode.getLocation());
            }
        }
        assert determinedNode != null;
        if (null != pendingNode) {
            pendingNode.setLocation(determinedNode.getLocation());
            pendingNode.setOutputDistribution(determinedNode.getOutputDistribution());
        }
        return simpleRelDerive(determinedNode);
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
