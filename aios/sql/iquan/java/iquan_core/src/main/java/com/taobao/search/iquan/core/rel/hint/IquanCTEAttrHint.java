package com.taobao.search.iquan.core.rel.hint;

import java.util.List;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.HintPredicate;
import org.apache.calcite.rel.hint.HintPredicates;
import org.apache.calcite.rel.hint.RelHint;

public class IquanCTEAttrHint implements IquanHint {

    public static final IquanCTEAttrHint INSTANCE = new IquanCTEAttrHint();
    private static List<HintPredicate> precedingPredicates = ImmutableList.of(HintPredicates.PROJECT);

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
