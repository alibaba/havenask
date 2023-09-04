package com.taobao.search.iquan.core.rel.hint;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.rel.hint.HintStrategyTable;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

public class IquanHintInitializer {
    static final IquanHint[] iquanHintList = {
            IquanJoinHint.HASH_JOIN,
            IquanJoinHint.LOOKUP_JOIN,
            IquanNoIndexHint.INSTANCE,
            IquanLocalParallelHint.INSTANCE,
            IquanNormalAggHint.INSTANCE,
            IquanScanAttrHint.INSTANCE,
            IquanJoinAttrHint.INSTANCE,
            IquanAggAttrHint.INSTANCE,
            IquanCTEAttrHint.INSTANCE
    };

    public static final Map<String, IquanHint> iquanHintTable;
    static {
        ImmutableMap.Builder<String, IquanHint> builder = new ImmutableMap.Builder<>();
        Arrays.stream(iquanHintList).forEach(v -> builder.put(v.getName(), v));
        iquanHintTable = builder.build();
    }

    public static final HintStrategyTable hintStrategyTable;
    static {
        HintStrategyTable.Builder builder = HintStrategyTable.builder();
        Arrays.stream(iquanHintList).forEach(v -> builder.hintStrategy(v.getName(), v));
        hintStrategyTable = builder.build();
    }

    public static final Set<String> iquanPropagateHints;
    static {
        iquanPropagateHints = ImmutableSet.of(IquanNormalAggHint.INSTANCE.getName(),
                IquanJoinAttrHint.INSTANCE.getName(),
                IquanAggAttrHint.INSTANCE.getName()
                );
    }
}
