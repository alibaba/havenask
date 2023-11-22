package com.taobao.search.iquan.core.rel.visitor.relshuttle;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;

public class SameRelExpandShuttle extends RelHomogeneousShuttle {
    private Set<RelNode> visitedNode = new HashSet<>();

    @Override
    public RelNode visit(RelNode rel) {
        boolean visted = !visitedNode.add(rel);
        boolean change = false;
        List<RelNode> newInputs = new ArrayList<>(rel.getInputs().size());

        for (RelNode node : rel.getInputs()) {
            RelNode newNode = node.accept(this);
            change = change || node != newNode;
            newInputs.add(newNode);
        }
        return visted || change ? rel.copy(rel.getTraitSet(), newInputs) : rel;
    }
}
