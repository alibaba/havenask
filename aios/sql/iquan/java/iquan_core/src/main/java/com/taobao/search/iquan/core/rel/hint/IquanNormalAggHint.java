package com.taobao.search.iquan.core.rel.hint;

import com.taobao.search.iquan.core.common.ConstantDefine;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.HintPredicate;
import org.apache.calcite.rel.hint.HintPredicates;
import org.apache.calcite.rel.hint.RelHint;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class IquanNormalAggHint implements IquanHint {
    private static List<HintPredicate> precedingPredicates = ImmutableList.of(HintPredicates.AGGREGATE);

    public static final IquanNormalAggHint INSTANCE = new IquanNormalAggHint(ConstantDefine.NORMAL_AGG_HINT);

    private final String name;

    public IquanNormalAggHint(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public IquanHintCategory getCategory() {
        return IquanHintCategory.CAT_AGG;
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
