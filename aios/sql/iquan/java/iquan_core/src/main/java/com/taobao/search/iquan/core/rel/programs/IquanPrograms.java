package com.taobao.search.iquan.core.rel.programs;

import com.taobao.search.iquan.core.api.common.OptimizeLevel;
import com.taobao.search.iquan.core.rel.IquanRelBuilder;
import com.taobao.search.iquan.core.rel.convert.physical.*;
import com.taobao.search.iquan.core.rel.rules.logical.*;
import com.taobao.search.iquan.core.rel.rules.logical.calcite.ReduceExpressionsRule;
import com.taobao.search.iquan.core.rel.rules.logical.calcite.*;
import com.taobao.search.iquan.core.rel.rules.logical.layer.table.*;
import com.taobao.search.iquan.core.rel.rules.physical.*;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.rules.*;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.commons.collections4.IteratorUtils;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.List;

public class IquanPrograms {
    public static final String FUSE_LAYER_TABLE_PREPARE = "fuse layer table prepare";
    public static final String LOGICAL_OPTIMIZE = "logical optimize";
    public static final String PHYSICAL_CONVERT = "physical convert";
    public static final String PHYSICAL_OPTIMIZE = "physical optimize";

    public static List<Pair<String, HepProgram>> fuseLayerTableRules() {
        List<Pair<String, HepProgram>> ruleSetList = new ArrayList<>();
        RuleSet multiJoinRules = RuleSets.ofList(
                IquanMultiJoinRule.INSTANCE,
                IquanMultiJoinErrorRule.INSTANCE,
                IquanTvfModifyRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule()
        );
        ruleSetList.add(new Pair<>("multiJoin process", buildHepProgramSequence(multiJoinRules, HepMatchOrder.BOTTOM_UP)));

        RuleSet subQueryToSemiJoinRules = RuleSets.ofList(
                FilterToSemiAntiJoinRule.INSTANCE,
                JoinPushExpressionsRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                SubQueryRemoveRule.Config.FILTER.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                SubQueryRemoveRule.Config.PROJECT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                SubQueryRemoveRule.Config.JOIN.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule()
        );
        ruleSetList.add(new Pair<>("rewrite sub-queries to semi-join", buildHepProgramSequence(subQueryToSemiJoinRules, HepMatchOrder.BOTTOM_UP)));

        RuleSet removeSubQueryRules = RuleSets.ofList(
                //sub-queries remove
                SubQueryRemoveRule.Config.FILTER.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                SubQueryRemoveRule.Config.PROJECT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                SubQueryRemoveRule.Config.JOIN.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule()
        );
        ruleSetList.add(new Pair<>("sub-queries remove", buildHepProgram(removeSubQueryRules, HepMatchOrder.BOTTOM_UP)));

        RuleSet rules = RuleSets.ofList(
                IquanJoinCondTypeCoerceRule.INSTANCE,
                IquanFilterJoinRule.FILTER_ON_JOIN,
                IquanFilterJoinRule.JOIN,
                JoinPushExpressionsRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                ProjectJoinRule.INSTANCE,
                FilterAggregateTransposeRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                IquanFilterProjectTransposeRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                FilterSetOpTransposeRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                IquanFilterMergeRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                FilterEliminateCteRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                JoinEliminateCteRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                IquanProjectMergeRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule()
                );
        ruleSetList.add(new Pair<>("fuse layer table prepare", buildHepProgram(rules, HepMatchOrder.BOTTOM_UP)));

        RuleSet fuseRules = RuleSets.ofList(
                ConvertFuseLayerTableRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                TakeInFuseLayerTableRule.Config.FILTER.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                TakeInFuseLayerTableRule.Config.PROJECT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule()
        );
        ruleSetList.add(new Pair<>("convert to fuseLayerTable and take in filters and projects", buildHepProgram(fuseRules, HepMatchOrder.BOTTOM_UP)));

        return ruleSetList;
    }

    public static List<Pair<String, HepProgram>> logicalRules(OptimizeLevel level) {
        List<Pair<String, HepProgram>> ruleSetList = new ArrayList<>();
        RuleSet fuseLayerTableRules = RuleSets.ofList(
                JoinLayerTableRule.INSTANCE,
                IquanSortFuseLayerTableRule.INSTANCE,
                IquanSortPushUnion.INSTANCE
        );
        ruleSetList.add(new Pair<>("fuse layer table rules", buildHepProgram(fuseLayerTableRules, HepMatchOrder.BOTTOM_UP)));

        RuleSet layerTableRules = RuleSets.ofList(
                FuseLayerTableScanExpandRule.INSTANCE
        );
        ruleSetList.add(new Pair<>("expand layerTable", buildHepProgramSequence(layerTableRules, HepMatchOrder.BOTTOM_UP)));

        RuleSet filterPrepareRules = RuleSets.ofList(
                IquanFilterJoinRule.FILTER_ON_JOIN,
                IquanFilterJoinRule.JOIN,
                FilterAggregateTransposeRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                IquanFilterProjectTransposeRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                FilterSetOpTransposeRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                IquanFilterMergeRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),

                JoinPushExpressionsRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),

                // Iquan REDUCE_EXPRESSION_RULES
                ReduceExpressionsRule.FILTER_INSTANCE,
                ReduceExpressionsRule.PROJECT_INSTANCE,
                ReduceExpressionsRule.CALC_INSTANCE,
                ReduceExpressionsRule.JOIN_INSTANCE
        );
        switch (level) {
            case O4:
                filterPrepareRules = RuleSets.ofList(
                        IquanFilterJoinRule.FILTER_ON_JOIN,
                        IquanFilterJoinRule.JOIN,
                        FilterAggregateTransposeRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                        IquanFilterProjectTransposeRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                        FilterSetOpTransposeRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                        IquanFilterMergeRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                        JoinPushExpressionsRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule()
                );
                break;
        }
        ruleSetList.add(new Pair<>("other predicate rewrite", buildHepProgram(filterPrepareRules, HepMatchOrder.BOTTOM_UP)));

        RuleSet logicalOptRules = RuleSets.ofList(
                ExtractNotInRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                IquanProjectFilterTransposeRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                ProjectJoinRule.INSTANCE,
                IquanProjectMergeRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),

                IquanProjectRemoveRule.INSTANCE,
                IquanProjectSortTransposeRule.INSTANCE,
                SortCombineRule.INSTANCE,
                AggregateProjectPullUpConstantsRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),

                PruneEmptyRules.RemoveEmptySingleRule.RemoveEmptySingleRuleConfig.AGGREGATE.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                PruneEmptyRules.RemoveEmptySingleRule.RemoveEmptySingleRuleConfig.FILTER.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                PruneEmptyRules.SortFetchZeroRuleConfig.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                PruneEmptyRules.JoinLeftEmptyRuleConfig.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                PruneEmptyRules.JoinRightEmptyRuleConfig.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                PruneEmptyRules.RemoveEmptySingleRule.RemoveEmptySingleRuleConfig.PROJECT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                PruneEmptyRules.RemoveEmptySingleRule.RemoveEmptySingleRuleConfig.SORT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                PruneEmptyRules.UnionEmptyPruneRuleConfig.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),

                // join rules
                JoinPushExpressionsRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                ProjectSetOpTransposeRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                // remove union with only a single child
                UnionEliminatorRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),

                // aggregate union rule
                AggregateUnionAggregateRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                // expand distinct aggregate to normal aggregate with groupby
                IquanAggregateExpandDistinctAggregatesRule.INSTANCE,
                // reduce useless aggCall
                IquanPruneAggregateCallRule.PROJECT_ON_AGGREGATE,
                // remove unnecessary sort rule
                SortRemoveRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),

                // merge multiple union
                LogicalUnionMergeRule.INSTANCE,
                IquanProjectTvfTransposeRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                IquanSetOpTypeCoerceRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule()
        );
        ruleSetList.add(new Pair<>("logical optimize", buildHepProgram(logicalOptRules, HepMatchOrder.BOTTOM_UP)));

        RuleSet calcOptRules = RuleSets.ofList(
                IquanFilterCalcMergeRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                IquanProjectCalcMergeRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                FilterToCalcRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                ProjectToCalcRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                IquanCalcMergeRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                IquanPruneAggregateCallRule.CALC_ON_AGGREGATE
        );
        ruleSetList.add(new Pair<>("calc optimize", buildHepProgram(calcOptRules, HepMatchOrder.BOTTOM_UP)));

        RuleSet deSearch = RuleSets.ofList(
                IquanDeSearchRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule(),
                IquanToIquanInNotIquanInRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule()
        );
        ruleSetList.add((new Pair<>("de search op", buildHepProgramSequence(deSearch, HepMatchOrder.BOTTOM_UP))));

        return ruleSetList;
    }

    public static List<Pair<String, HepProgram>> physicalConverterRules(OptimizeLevel level, boolean enableTurboJet) {
        List<Pair<String, HepProgram>> ruleSetList = new ArrayList<>();
        List<RelOptRule> rules = new ArrayList<>();

        rules.add(IquanTableModifyConverterRule.INSTANCE);
        rules.add(UncollectMergeRule.INSTANCE);
        rules.add(IquanAggregateConverterRule.INSTANCE);
        rules.add(IquanGroupingSetsConverterRule.INSTANCE);
        rules.add(IquanCorrelateConverterRule.INSTANCE);
        rules.add(IquanJoinConverterRule.INSTANCE);
        rules.add(IquanCalcConverterRule.INSTANCE);
        rules.add(IquanCalcMergeRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule());
        rules.add(IquanSortConverterRule.INSTANCE);
        rules.add(IquanTableScanConverterRule.INSTANCE);
        rules.add(IquanTableFunctionScanConverterRule.INSTANCE);
        rules.add(IquanMatchConverterRule.INSTANCE);
        rules.add(IquanUnionConverterRule.INSTANCE);
        rules.add(IquanValuesConverterRule.INSTANCE);
        rules.add(IquanMultiJoinConverterRule.INSTANCE);
        rules.add(IquanCTEConverterRule.INSTANCE);
        if (!enableTurboJet) {
            rules.add(CalcScanMergeRule.INSTANCE);
        }

        ruleSetList.add(new Pair<>("physical converter", buildHepProgramSequence(RuleSets.ofList(rules), HepMatchOrder.BOTTOM_UP)));
        return ruleSetList;
    }

    public static List<Pair<String, HepProgram>> physicalRules(OptimizeLevel level, boolean enableTurboJet) {
        List<Pair<String, HepProgram>> ruleSetList = new ArrayList<>();
        // Rule orders is important, not change!!!
        List<RelOptRule> rules = new ArrayList<>();
        rules.add(CalcUncollectMergeRule.INSTANCE);
        rules.add(CorrelateUncollectMergeRule.INSTANCE);
        rules.add(ExecNonEquiJoinRule.INSTANCE);
        rules.add(ExecEquiJoinRule.INSTANCE);
        rules.add(ExecSemiJoinRule.INSTANCE);
        rules.add(ExecLookupJoinMergeRule.INSTANCE);
        rules.add(ExecMultiLookupJoinRule.INSTANCE);
        rules.add(IquanCalcMergeRule.Config.DEFAULT.withRelBuilderFactory(IquanRelBuilder.LOGICAL_BUILDER).toRule());

        if (!enableTurboJet) {
            rules.add(CalcScanMergeRule.INSTANCE);
            rules.add(MatchTypeRankScanMergeRule.INSTANCE);
            rules.add(SortScanMergeRule.INSTANCE);
        }

        rules.add(PushDownOpRewriteRule.INSTANCE);

        if (enableTurboJet) {
            rules.add(TurboJetCalcRewriteRule.INSTANCE);
        }
        rules.add(DistinctAggIndexOptimizeRule.INSTANCE);
        rules.add(GlobalAggIndexOptimizeRule.INSTANCE);
        rules.add(AggIndexPushDownRule.INSTANCE);
        ruleSetList.add(new Pair<>("physical transformation", buildHepProgramSequence(RuleSets.ofList(rules), HepMatchOrder.BOTTOM_UP)));
        return ruleSetList;
    }

    private static HepProgram buildHepProgram(RuleSet rules, HepMatchOrder matchOrder) {
        HepProgramBuilder builder = new HepProgramBuilder();
        return builder.addMatchLimit(Integer.MAX_VALUE)
                .addMatchOrder(matchOrder)
                .addRuleCollection(IteratorUtils.toList(rules.iterator()))
                .build();
    }

    private static HepProgram buildHepProgramSequence(RuleSet rules, HepMatchOrder matchOrder) {
        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addMatchLimit(Integer.MAX_VALUE)
                .addMatchOrder(matchOrder);
        for (RelOptRule rule : rules) {
            builder.addRuleInstance(rule);
        }
        return builder.build();
    }
}