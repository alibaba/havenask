package com.taobao.search.iquan.core.rel.hint;

import com.taobao.search.iquan.core.api.config.IquanConfigManager;
import com.taobao.search.iquan.core.api.config.SqlConfigOptions;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.utils.CatalogUtils;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.HintPredicate;
import org.apache.calcite.rel.hint.RelHint;

import java.util.List;
import java.util.Objects;


public class IquanJoinHint implements IquanHint {
    private static List<HintPredicate> precedingPredicates = IquanScanAttrHint.precedingPredicates;

    static final IquanJoinHint HASH_JOIN = new IquanJoinHint(ConstantDefine.HASH_JOIN_HINT);
    static final IquanJoinHint LOOKUP_JOIN = new IquanJoinHint(ConstantDefine.LOOKUP_JOIN_HINT);

    private final String name;

    public IquanJoinHint(String name) {
        this.name = name;
    }

    @Override
    public IquanHintCategory getCategory() {
        return IquanHintCategory.CAT_JOIN;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public List<HintPredicate> getPrecedingPredicates() {
        return precedingPredicates;
    }

    @Override
    public boolean doApply(RelHint relHint, RelNode relNode) {
        IquanConfigManager conf = IquanRelOptUtils.getConfigFromRel(relNode);
        String defaultCatalogName = conf.getString(SqlConfigOptions.IQUAN_DEFAULT_CATALOG_NAME);
        String defaultDbName = conf.getString(SqlConfigOptions.IQUAN_DEFAULT_DB_NAME);

        List<String> actualTablePath = CatalogUtils.getTablePath(relNode);
        if (actualTablePath.size() != 3) {
            throw new RuntimeException();
        }

        List<String> hintOptions = relHint.listOptions;
        for (String hintOption : hintOptions) {
            List<String> hintTablePath = IquanHintOptUtils.getTablePath(defaultCatalogName, defaultDbName, hintOption);
            if (Objects.equals(actualTablePath, hintTablePath)) {
                return true;
            }
        }
        return false;
    }
}
