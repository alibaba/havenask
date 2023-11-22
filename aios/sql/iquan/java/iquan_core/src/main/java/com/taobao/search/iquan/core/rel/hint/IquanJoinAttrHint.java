package com.taobao.search.iquan.core.rel.hint;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.taobao.search.iquan.core.common.ConstantDefine;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.HintPredicate;
import org.apache.calcite.rel.hint.HintPredicates;
import org.apache.calcite.rel.hint.RelHint;

public class IquanJoinAttrHint implements IquanHint {
    public static final IquanJoinAttrHint INSTANCE = new IquanJoinAttrHint(ConstantDefine.JOIN_ATTR_HINT);
    private static List<HintPredicate> precedingPredicates = ImmutableList.of(HintPredicates.JOIN);
    private final String name;

    public IquanJoinAttrHint(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public IquanHintCategory getCategory() {
        return IquanHintCategory.CAT_JOIN_ATTR;
    }

    @Override
    public List<HintPredicate> getPrecedingPredicates() {
        return precedingPredicates;
    }

    @Override
    public boolean doApply(RelHint relHint, RelNode relNode) {
        Map<String, String> hintOptions = relHint.kvOptions;
        return !hintOptions.isEmpty();
    }
}
