package com.taobao.search.iquan.core.rel.hint;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.ops.logical.LayerTable.LogicalLayerTableScan;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.HintPredicate;
import org.apache.calcite.rel.hint.RelHint;

public class IquanScanAttrHint implements IquanHint {
    public static final IquanScanAttrHint INSTANCE = new IquanScanAttrHint(ConstantDefine.SCAN_ATTR_HINT);
    public static List<HintPredicate> precedingPredicates = ImmutableList.of(new HintPredicate() {
        @Override
        public boolean apply(RelHint hint, RelNode rel) {
            return rel instanceof TableScan || rel instanceof LogicalLayerTableScan;
        }
    });
    private final String name;

    public IquanScanAttrHint(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public IquanHintCategory getCategory() {
        return IquanHintCategory.CAT_SCAN_ATTR;
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
