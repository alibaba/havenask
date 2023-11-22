package com.taobao.search.iquan.core.rel.hint;

import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.HintPredicate;
import org.apache.calcite.rel.hint.RelHint;

public interface IquanHint extends HintPredicate {
    String getName();

    IquanHintCategory getCategory();

    List<HintPredicate> getPrecedingPredicates();

    @Override
    default boolean apply(RelHint relHint, RelNode relNode) {
        if (getPrecedingPredicates().stream().anyMatch(v -> !v.apply(relHint, relNode))) {
            return false;
        }
        return doApply(relHint, relNode);
    }

    boolean doApply(RelHint relHint, RelNode relNode);
}