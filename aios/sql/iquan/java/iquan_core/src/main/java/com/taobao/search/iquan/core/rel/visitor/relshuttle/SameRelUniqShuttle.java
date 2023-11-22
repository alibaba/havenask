package com.taobao.search.iquan.core.rel.visitor.relshuttle;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.plan.RelDigest;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;

public class SameRelUniqShuttle extends RelHomogeneousShuttle {

    private Map<RelDigest, RelNode> digestToNode = new HashMap<>();

    @Override
    public RelNode visit(RelNode rel) {
        List<RelNode> inputs = rel.getInputs();
        if (inputs.isEmpty()) {
            return uniqByDigest(rel);
        }
        List<RelNode> newInputs = new ArrayList<>(inputs.size());
        boolean hasDiff = false;
        for (RelNode input : inputs) {
            RelNode newInput = input.accept(this);
            if (newInput != input) {
                hasDiff = true;
            }
            newInputs.add(newInput);
        }
        if (hasDiff) {
            rel = rel.copy(rel.getTraitSet(), newInputs);
        }
        return uniqByDigest(rel);
    }

    private RelNode uniqByDigest(RelNode rel) {
        RelDigest digest = rel.getRelDigest();
        return digestToNode.computeIfAbsent(digest, k -> rel);
    }
}
