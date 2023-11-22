package com.taobao.search.iquan.core.rel.programs;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.taobao.search.iquan.core.api.config.IquanConfigManager;
import com.taobao.search.iquan.core.api.config.SqlConfigOptions;
import com.taobao.search.iquan.core.rel.ops.logical.CTEConsumer;
import com.taobao.search.iquan.core.rel.ops.logical.CTEProducer;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CTEOptimizer {

    private static final Logger logger = LoggerFactory.getLogger(CTEOptimizer.class);
    private final Function<RelNode, RelNode> optimizer;
    private final int cteOptVer;
    private final RelBuilder relBuilder;
    private final RexBuilder rexBuilder;
    private final TreeMap<Integer, RelNode> optRoots = new TreeMap<>();
    private final TreeMap<Integer, CTEDepend> relDeps = new TreeMap<>();
    private final IdentityHashMap<LogicalCalc, RelNode> relRewrites = new IdentityHashMap<>();
    public CTEOptimizer(
            RelOptCluster cluster,
            Function<RelNode, RelNode> optimizer,
            IquanConfigManager config) {
        this.optimizer = optimizer;
        this.cteOptVer = config.getInteger(SqlConfigOptions.IQUAN_CTE_OPT_VER);
        this.relBuilder = RelFactories.LOGICAL_BUILDER.create(cluster, null);
        this.rexBuilder = cluster.getRexBuilder();
    }

    public RelNode optmize(RelNode rootNode) {
        subOptimize(rootNode);
        return applyOptimize();
    }

    private void subOptimize(RelNode rootNode) {
        RelNode curNode = rootNode;
        int rootIndex = Integer.MAX_VALUE;
        while (curNode != null) {
            logger.debug("logical optimize sub root[{}]", rootIndex);
            RelNode optNode = optimize(curNode);
            optRoots.put(rootIndex, optNode);
            DependCollector collector = new DependCollector();
            collector.go(optNode);
            Map.Entry<Integer, CTEDepend> entry = relDeps.lowerEntry(rootIndex);
            if (entry == null) {
                curNode = null;
            } else {
                curNode = pushDownDepend(entry.getValue());
                rootIndex = entry.getKey();
            }
        }
    }

    private RelNode optimize(RelNode node) {
        return optimizer.apply(node);
    }

    private RelNode pushDownDepend(CTEDepend dep) {
        assert !dep.deps.isEmpty() : "empty dep list";
        if (dep.deps.size() == 1) {
            return pushCalcPastCTE(dep.deps.get(0), dep.cte);
        } else {
            MergeCalcPastCTE pusher = new MergeCalcPastCTE();
            return pusher.process(dep.deps, dep.cte);
        }
    }

    private RelNode pushCalcPastCTE(LogicalCalc calc, CTEConsumer consumer) {
        if (calc == null) {
            return consumer.getProducer();
        }
        Pair<ImmutableList<RexNode>, ImmutableList<RexNode>> projectFilter =
                calc.getProgram().split();
        CTEProducer producer = consumer.getProducer();
        RelNode newInput = relBuilder.push(producer.getInput())
                .filter(projectFilter.right)
                .project(projectFilter.left, calc.getRowType().getFieldNames(), true)
                .build();
        CTEProducer newProducer = (CTEProducer) producer.copy(
                producer.getTraitSet(),
                Collections.singletonList(newInput));
        CTEConsumer newConsumer = new CTEConsumer(
                consumer.getCluster(),
                consumer.getTraitSet(),
                newProducer);
        relRewrites.put(calc, newConsumer);
        return newProducer;
    }

    private RelNode applyOptimize() {
        assert !optRoots.isEmpty() : "empty sub roots";
        ApplyRewriteShuttle shuttle = new ApplyRewriteShuttle();
        Iterator<Map.Entry<Integer, RelNode>> iterator =
                optRoots.entrySet().iterator();
        Map.Entry<Integer, RelNode> curEntry = iterator.next();
        while (iterator.hasNext()) {
            curEntry = iterator.next();
            RelNode newRoot = curEntry.getValue().accept(shuttle);
            curEntry.setValue(newRoot);
        }
        return curEntry.getValue();
    }

    private static class CTEDepend {

        public final CTEConsumer cte;
        public final List<LogicalCalc> deps = new ArrayList<>();

        public CTEDepend(CTEConsumer cte) {
            this.cte = cte;
        }
    }

    class DependCollector extends RelVisitor {

        @Override
        public void visit(RelNode node, int ordinal, RelNode parent) {
            if (!collect(node, ordinal)) {
                super.visit(node, ordinal, parent);
            }
        }

        private boolean collect(RelNode node, int ordinal) {
            if (node instanceof CTEConsumer) {
                addDepend((CTEConsumer) node, null);
                return true;
            }
            if (!(node instanceof LogicalCalc)) {
                return false;
            }
            LogicalCalc calc = (LogicalCalc) node;
            RelNode child = calc.getInput();
            if (!(child instanceof CTEConsumer)) {
                return false;
            }
            CTEConsumer cte = (CTEConsumer) child;
            addDepend(cte, calc);
            return true;
        }

        private void addDepend(CTEConsumer cte, LogicalCalc calc) {
            relDeps.compute(
                    cte.getProducer().getIndex(),
                    (k, v) -> {
                        if (v == null) {
                            v = new CTEDepend(cte);
                        }
                        v.deps.add(calc);
                        return v;
                    });
        }
    }

    private class MergeCalcPastCTE {

        List<List<RexNode>> filtersList;
        List<List<RexNode>> projectsList;
        List<List<RexNode>> newFiltersList;
        RexNode pushFilter = null;
        Map<Integer, Integer> refMapping;
        CTEProducer newProducer = null;
        CTEConsumer newConsumer = null;

        public RelNode process(List<LogicalCalc> calcList, CTEConsumer consumer) {
            CTEProducer producer = consumer.getProducer();
            if (calcList.contains(null)) {
                return producer;
            }
            expandCalc(calcList);
            initFilterPush();
            if (!pushProjectFilter(consumer)) {
                return producer;
            }
            updateRemainCalc(calcList);
            return newProducer;
        }

        private void expandCalc(List<LogicalCalc> calcList) {
            int relSize = calcList.size();
            filtersList = new ArrayList<>(relSize);
            projectsList = new ArrayList<>(relSize);
            calcList.stream()
                    .map(calc -> calc.getProgram().split())
                    .forEach(projectFilter -> {
                        filtersList.add(projectFilter.right);
                        projectsList.add(projectFilter.left);
                    });
        }

        private void initFilterPush() {
            newFiltersList = filtersList;
            if (filtersList.stream().anyMatch(List::isEmpty)) {
                return;
            }
            List<List<Pair<RexNode, String>>> keysCache = new ArrayList<>(filtersList.size());
            Set<Pair<RexNode, String>> hashSet = intersectFilters(keysCache);
            if (hashSet == null) {
                return;
            }
            newFiltersList = new ArrayList<>(filtersList.size());
            for (int i = 0; i < filtersList.size(); ++i) {
                List<RexNode> curFilters = filtersList.get(i);
                List<Pair<RexNode, String>> curKeys = keysCache.get(i);
                List<RexNode> newFilters = new ArrayList<>(curFilters.size());
                newFiltersList.add(newFilters);
                for (int j = 0; j < curFilters.size(); ++j) {
                    if (!hashSet.contains(curKeys.get(j))) {
                        newFilters.add(curFilters.get(j));
                    }
                }
            }
            List<RexNode> pushFilters = hashSet.stream()
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
            pushFilter = mergeFilters(pushFilters);
        }

        private RexNode mergeFilters(List<RexNode> filters) {
            switch (filters.size()) {
                case 0:
                    return null;
                case 1:
                    return filters.get(0);
                default:
                    return rexBuilder.makeCall(SqlStdOperatorTable.AND, filters);
            }
        }

        private Set<Pair<RexNode, String>> intersectFilters(List<List<Pair<RexNode, String>>> keysCache) {
            List<Pair<RexNode, String>> filterKeys = makeRexKeys(filtersList.get(0), keysCache);
            Set<Pair<RexNode, String>> hashSet = new LinkedHashSet<>(filterKeys);
            hashSet.remove(null);
            for (int i = 1; i < filtersList.size(); ++i) {
                filterKeys = makeRexKeys(filtersList.get(i), keysCache);
                Set<Pair<RexNode, String>> newHashSet = new LinkedHashSet<>(hashSet.size());
                for (Pair<RexNode, String> key : filterKeys) {
                    if (hashSet.contains(key)) {
                        newHashSet.add(key);
                    }
                }
                if (newHashSet.isEmpty()) {
                    return null;
                }
                hashSet = newHashSet;
            }
            return hashSet;
        }

        private List<Pair<RexNode, String>> makeRexKeys(
                List<RexNode> rexNodes,
                List<List<Pair<RexNode, String>>> keyCache) {
            List<Pair<RexNode, String>> keys = rexNodes.stream()
                    .map(o -> {
                        if (RexUtil.isDeterministic(o)) {
                            return RexUtil.makeKey(o);
                        } else {
                            return null;
                        }
                    })
                    .collect(Collectors.toList());
            keyCache.add(keys);
            return keys;
        }

        private boolean pushProjectFilter(CTEConsumer consumer) {
            int fieldCount = consumer.getRowType().getFieldCount();
            initInputRefMapping(fieldCount);
            if (pushFilter == null && refMapping.size() >= fieldCount) {
                return false;
            }
            CTEProducer producer = consumer.getProducer();
            RelNode input = producer.getInput();
            relBuilder.push(input);
            if (pushFilter != null) {
                relBuilder.filter(pushFilter);
            }
            if (refMapping.size() < fieldCount) {
                List<RexNode> pushProjects = new ArrayList<>(fieldCount);
                for (Integer index : refMapping.keySet()) {
                    pushProjects.add(rexBuilder.makeInputRef(input, index));
                }
                relBuilder.project(pushProjects);
            }
            RelNode newInput = relBuilder.build();
            newProducer = (CTEProducer) producer.copy(
                    producer.getTraitSet(),
                    Collections.singletonList(newInput));
            newConsumer = new CTEConsumer(
                    consumer.getCluster(),
                    consumer.getTraitSet(),
                    newProducer);
            return true;
        }

        private void initInputRefMapping(int fieldCount) {
            BitSet inputRefs = new BitSet(fieldCount);
            RexVisitor<Void> visitor = new RexVisitorImpl<Void>(true) {
                @Override
                public Void visitInputRef(RexInputRef inputRef) {
                    inputRefs.set(inputRef.getIndex());
                    return null;
                }
            };
            for (List<RexNode> filters : newFiltersList) {
                RexUtil.apply(visitor, filters, null);
            }
            for (List<RexNode> projects : projectsList) {
                RexUtil.apply(visitor, projects, null);
            }
            refMapping = new LinkedHashMap<>(fieldCount);
            for (int i = inputRefs.nextSetBit(0), j = 0;
                 i >= 0;
                 i = inputRefs.nextSetBit(i + 1), ++j) {
                refMapping.put(i, j);
            }
        }

        private List<RexNode> shuttleRexList(RexShuttle shuttle, List<RexNode> nodes) {
            return nodes.stream()
                    .map(node -> node.accept(shuttle))
                    .collect(Collectors.toList());
        }

        private void updateRemainCalc(List<LogicalCalc> calcList) {
            RexShuttle shuttle = new RexShuttle() {
                @Override
                public RexNode visitInputRef(RexInputRef inputRef) {
                    return rexBuilder.makeInputRef(
                            newConsumer,
                            refMapping.get(inputRef.getIndex()));
                }
            };

            for (int i = 0; i < calcList.size(); ++i) {
                LogicalCalc calc = calcList.get(i);
                List<RexNode> filters = shuttleRexList(shuttle, newFiltersList.get(i));
                RexNode filter = mergeFilters(filters);
                List<RexNode> projects = shuttleRexList(shuttle, projectsList.get(i));
                RexProgram program = RexProgram.create(
                        newConsumer.getRowType(), projects, filter, calc.getRowType(), rexBuilder);
                LogicalCalc newCalc = LogicalCalc.create(newConsumer, program);
                relRewrites.put(calc, newCalc);
            }
        }
    }

    class ApplyRewriteShuttle extends RelShuttleImpl {

        @Override
        public RelNode visit(LogicalCalc calc) {
            RelNode rewriteNode = relRewrites.get(calc);
            if (rewriteNode == null) {
                return super.visit(calc);
            }
            return rewriteNode.accept(this);
        }

        private RelNode visit(CTEConsumer consumer) {
            CTEProducer producer = consumer.getProducer();
            int index = producer.getIndex();
            RelNode optRoot = optRoots.get(index);
            assert optRoot instanceof CTEProducer : "invalid cte root";
            CTEProducer optProducer = (CTEProducer) optRoot;
            if (cteOptVer == 0) {
                return optProducer.getInput();
            }
            CTEDepend depend = relDeps.get(index);
            assert depend != null : "invalid cte depend";
            if (enableCTEInline(depend.deps.size(), optProducer.getHints())) {
                return optProducer.getInput();
            } else {
                return optProducer;
            }
        }

        private boolean enableCTEInline(int refCount, List<RelHint> hints) {
            boolean enableInline = (refCount == 1);
            for (RelHint hint : hints) {
                if (hint.kvOptions == null) {
                    continue;
                }
                String value = hint.kvOptions.get("inline");
                if (value == null) {
                    continue;
                }
                enableInline = Boolean.parseBoolean(value);
            }
            return enableInline;
        }

        @Override
        public RelNode visit(RelNode other) {
            if (other instanceof LogicalCalc) {
                return visit((LogicalCalc) other);
            }
            if (!(other instanceof CTEConsumer)) {
                return super.visit(other);
            }
            return visit((CTEConsumer) other);
        }
    }
}
