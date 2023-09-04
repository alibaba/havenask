package com.taobao.search.iquan.core.rel.ops.physical;

import com.taobao.search.iquan.core.api.config.IquanConfigManager;
import com.taobao.search.iquan.core.api.schema.Distribution;
import com.taobao.search.iquan.core.api.schema.Location;
import com.taobao.search.iquan.core.catalog.GlobalCatalog;
import com.taobao.search.iquan.core.rel.convention.IquanConvention;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.plan.PlanWriteUtils;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import com.taobao.search.iquan.core.utils.RelDistributionUtil;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Match;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.*;

public class IquanMatchOp extends Match implements IquanRelNode {
    private int parallelNum = -1;
    private int parallelIndex = -1;
    private Location location;
    private Distribution distribution;

    /**
     * Creates a IquanMatchOp.
     *
     * @param cluster            cluster
     * @param traitSet           Trait set
     * @param input              Input relational expression
     * @param rowType            Row type
     * @param pattern            Regular Expression defining pattern variables
     * @param strictStart        Whether it is a strict start pattern
     * @param strictEnd          Whether it is a strict end pattern
     * @param patternDefinitions Pattern definitions
     * @param measures           Measure definitions
     * @param after              After match definitions
     * @param subsets            Subset definitions
     * @param allRows            Whether all rows per match (false means one row per match)
     * @param partitionKeys      Partition by columns
     * @param orderKeys          Order by columns
     * @param interval           Interval definition, null if WITHIN clause is not defined
     */
    public IquanMatchOp(RelOptCluster cluster, RelTraitSet traitSet,
                        RelNode input, RelDataType rowType, RexNode pattern,
                        boolean strictStart, boolean strictEnd,
                        Map<String, RexNode> patternDefinitions, Map<String, RexNode> measures,
                        RexNode after, Map<String, ? extends SortedSet<String>> subsets,
                        boolean allRows, ImmutableBitSet partitionKeys, RelCollation orderKeys,
                        RexNode interval) {
        super(cluster, traitSet, input, rowType, pattern, strictStart, strictEnd,
                patternDefinitions, measures, after, subsets, allRows, partitionKeys,
                orderKeys, interval);
    }

    /**
     * Creates a IquanMatchOp.
     */
    public static IquanMatchOp create(RelNode input, RelDataType rowType,
                                      RexNode pattern, boolean strictStart, boolean strictEnd,
                                      Map<String, RexNode> patternDefinitions, Map<String, RexNode> measures,
                                      RexNode after, Map<String, ? extends SortedSet<String>> subsets, boolean allRows,
                                      ImmutableBitSet partitionKeys, RelCollation orderKeys, RexNode interval) {
        final RelOptCluster cluster = input.getCluster();
        final RelTraitSet traitSet = cluster.traitSetOf(IquanConvention.PHYSICAL);
        return new IquanMatchOp(cluster, traitSet, input, rowType, pattern,
                strictStart, strictEnd, patternDefinitions, measures, after, subsets,
                allRows, partitionKeys, orderKeys, interval);
    }

    //~ Methods ------------------------------------------------------

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        IquanMatchOp rel = new IquanMatchOp(getCluster(), traitSet, sole(inputs), getRowType(),
                pattern, strictStart, strictEnd, patternDefinitions, measures,
                after, subsets, allRows, partitionKeys, orderKeys,
                interval);
        rel.setParallelNum(parallelNum);
        rel.setParallelIndex(parallelIndex);
        return rel;
    }

    public void explainInternal(final Map<String, Object> map, SqlExplainLevel level) {
        final List<RexNode> emptyExprs = new ArrayList<>();
        final RelDataType inputRowType = input.getRowType();

        {
            final Map<String, Object> measuresMap = new TreeMap<>();
            measures.forEach(
                    (name, measure) -> IquanRelOptUtils.addMapIfNotEmpty(measuresMap, name,
                            PlanWriteUtils.formatExprImpl(measure, emptyExprs, inputRowType))
            );
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.MATCH_MEASURES, measuresMap);
        }
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.MATCH_PATTERN,
                PlanWriteUtils.formatExprImpl(pattern, emptyExprs, inputRowType)
        );
        map.put(ConstantDefine.MATCH_STRICT_START, strictStart);
        map.put(ConstantDefine.MATCH_STRICT_END, strictEnd);
        map.put(ConstantDefine.MATCH_ALL_ROWS, allRows);

        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.MATCH_AFTER,
                PlanWriteUtils.formatExprImpl(after, emptyExprs, inputRowType)
        );

        {
            final Map<String, Object> patternDefinitionsMap = new TreeMap<>();
            patternDefinitions.forEach(
                    (name, patternDefinition) -> IquanRelOptUtils.addMapIfNotEmpty(patternDefinitionsMap, name,
                            PlanWriteUtils.formatExprImpl(patternDefinition, emptyExprs, inputRowType))
            );
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.MATCH_PATTERN_DEFINITIONS, patternDefinitionsMap);
        }

        {
            final List<Object> aggregateCallsList = new ArrayList<>();
            aggregateCalls.forEach(aggregateCall -> IquanRelOptUtils.addListIfNotEmpty(aggregateCallsList,
                    PlanWriteUtils.formatExprImpl(aggregateCall, emptyExprs, inputRowType))
            );
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.MATCH_AGGREGATE_CALLS, aggregateCallsList);
        }

        {
            final List<Object> aggregateCallsPreVarList = new ArrayList<>();
            for (String key : aggregateCallsPreVar.keySet()) {
                final List<Object> innerList = new ArrayList<>();
                SortedSet<RexMRAggCall> callsPreVar = aggregateCallsPreVar.get(key);
                callsPreVar.forEach(aggregateCall -> IquanRelOptUtils.addListIfNotEmpty(innerList,
                        PlanWriteUtils.formatExprImpl(aggregateCall, emptyExprs, inputRowType))
                );
                IquanRelOptUtils.addListIfNotEmpty(aggregateCallsPreVarList, innerList);
            }
            IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.MATCH_AGGREGATE_CALLS_PRE_VAR, aggregateCallsPreVarList);
        }

        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.MATCH_SUBSETS, subsets);
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.MATCH_PARTITION_KEYS, partitionKeys.toString());
        PlanWriteUtils.formatCollations(map, orderKeys, inputRowType);
        IquanRelOptUtils.addMapIfNotEmpty(map, ConstantDefine.MATCH_INTERVAL,
                PlanWriteUtils.formatExprImpl(interval, emptyExprs, inputRowType)
        );
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        // Note: can't call super.explainTerms(pw), copy code from SingleRel.explainTerms()
        pw.input("input", this.getInput());

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
        return "MatchOp";
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
        IquanRelNode input = RelDistributionUtil.checkIquanRelType(inputs.get(0));
        return simpleRelDerive(input);
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
