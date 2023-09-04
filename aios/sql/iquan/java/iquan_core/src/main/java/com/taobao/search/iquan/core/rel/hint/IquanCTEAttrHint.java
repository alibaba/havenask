package com.taobao.search.iquan.core.rel.hint;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.HintPredicate;
import org.apache.calcite.rel.hint.HintPredicates;
import org.apache.calcite.rel.hint.RelHint;

import java.util.List;

public class IquanCTEAttrHint implements IquanHint {

    private static List<HintPredicate> precedingPredicates = ImmutableList.of(HintPredicates.PROJECT);

    public static final IquanCTEAttrHint INSTANCE = new IquanCTEAttrHint();

    @Override
    public String getName() {
        return "CTE_ATTR";
    }

    @Override
    public IquanHintCategory getCategory() {
        return IquanHintCategory.CAT_CTE_ATTR;
    }

    @Override
    public List<HintPredicate> getPrecedingPredicates() {
        return precedingPredicates;
    }

    @Override
    public boolean doApply(RelHint relHint, RelNode relNode) {
        return true;
    }
}
