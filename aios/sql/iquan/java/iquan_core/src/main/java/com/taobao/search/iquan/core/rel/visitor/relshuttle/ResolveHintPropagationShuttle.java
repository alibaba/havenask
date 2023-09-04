package com.taobao.search.iquan.core.rel.visitor.relshuttle;

import com.taobao.search.iquan.core.rel.hint.IquanHintInitializer;
import com.taobao.search.iquan.core.common.ConstantDefine;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.util.Pair;

import java.util.*;
import java.util.stream.Collectors;

// Not support Multi-Trees
public class ResolveHintPropagationShuttle extends RelHomogeneousShuttle {
    /**
     * Stack recording the hints and its current inheritPath.
     */
    private final Deque<Pair<List<RelHint>, Deque<Integer>>> inheritPaths =
            new ArrayDeque<>();

    /**
     * Visits a particular child of a parent.
     */
    protected RelNode visitChild(RelNode parent, int i, RelNode child) {
        inheritPaths.forEach(inheritPath -> inheritPath.right.push(i));
        try {
            RelNode child2 = child.accept(this);
            if (child2 != child) {
                final List<RelNode> newInputs = new ArrayList<>(parent.getInputs());
                newInputs.set(i, child2);
                return parent.copy(parent.getTraitSet(), newInputs);
            }
            return parent;
        } finally {
            inheritPaths.forEach(inheritPath -> inheritPath.right.pop());
        }
    }

    @Override
    public RelNode visit(RelNode other) {
        if (other instanceof LogicalAggregate || other instanceof LogicalJoin) {
            return visitHintable(other);
        } else {
            return visitChildren(other);
        }
    }

    public RelNode visitHintable(RelNode node) {
        final List<RelHint> removeHints = getRemoveHints((Hintable) node);
        final boolean hasHints = removeHints != null && removeHints.size() > 0;
        if (hasHints) {
            inheritPaths.push(Pair.of(removeHints, new ArrayDeque<>()));
        }
        final RelNode newNode = super.visit(node);
        if (hasHints) {
            inheritPaths.pop();
        }
        return doRemoveHints(newNode);
    }

    private List<RelHint> getRemoveHints(Hintable hintable) {
        if (hintable == null || hintable.getHints().isEmpty()) {
            return null;
        }
        final List<RelHint> hints = hintable.getHints();
        final List<RelHint> removeHints = new ArrayList<>(hints.size());
        for (RelHint hint : hints) {
            if (!(IquanHintInitializer.iquanPropagateHints.contains(hint.hintName))) {
                continue;
            }
            String propScope = hint.kvOptions.get(ConstantDefine.HINT_PROP_SCOPE_KEY);
            if (ConstantDefine.HINT_PROP_SCOPE_ALL_VALUE.equalsIgnoreCase(propScope)) {
                continue;
            }
            removeHints.add(hint);
        }
        return removeHints;
    }

    private RelNode doRemoveHints(RelNode node) {
        assert node instanceof Hintable;
        if (inheritPaths.size() > 0) {
            final List<RelHint> removeHints = inheritPaths.stream()
//                    .sorted(Comparator.comparingInt(o -> o.right.size()))
                    .map(path -> copyWithInheritPath(path.left, path.right))
                    .reduce(new ArrayList<>(), (acc, hints1) -> {
                        acc.addAll(hints1);
                        return acc;
                    });

            List<RelHint> originalHints = ((Hintable) node).getHints();
            List<RelHint> remainHints = originalHints.stream().filter(h -> !removeHints.contains(h)).collect(Collectors.toList());
            if (remainHints.size() != originalHints.size()) {
                return ((Hintable) node).withHints(remainHints);
            }
        }
        return node;
    }

    private static List<RelHint> copyWithInheritPath(List<RelHint> hints,
                                                     Deque<Integer> inheritPath) {
        final List<Integer> path = new ArrayList<>();
        final Iterator<Integer> iterator = inheritPath.descendingIterator();
        while (iterator.hasNext()) {
            path.add(iterator.next());
        }

        List<RelHint> newHints = new ArrayList<>(hints.size());
        for (RelHint hint : hints) {
            List<Integer> newInheritPath = new ArrayList<>(hint.inheritPath.size() + path.size());
            newInheritPath.addAll(hint.inheritPath);
            newInheritPath.addAll(path);
            newHints.add(hint.copy(newInheritPath));
        }
        return newHints;
    }

    public static List<RelNode> go(List<RelNode> roots) {
        List<RelNode> newRoots = new ArrayList<>();
        for (RelNode root : roots) {
            ResolveHintPropagationShuttle resolveHintPropagationShuttle = new ResolveHintPropagationShuttle();
            RelNode newRoot = root.accept(resolveHintPropagationShuttle);
            newRoots.add(newRoot);
        }
        return newRoots;
    }
}
