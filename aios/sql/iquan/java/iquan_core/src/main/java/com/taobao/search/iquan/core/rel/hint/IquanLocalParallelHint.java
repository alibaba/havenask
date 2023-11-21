package com.taobao.search.iquan.core.rel.hint;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.taobao.search.iquan.core.api.config.IquanConfigManager;
import com.taobao.search.iquan.core.api.config.SqlConfigOptions;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.HintPredicate;
import org.apache.calcite.rel.hint.RelHint;

public class IquanLocalParallelHint implements IquanHint {
    static final IquanLocalParallelHint INSTANCE = new IquanLocalParallelHint(ConstantDefine.LOCAL_PARALLEL_HINT);
    private static List<HintPredicate> precedingPredicates = IquanScanAttrHint.precedingPredicates;
    private final String name;

    public IquanLocalParallelHint(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public IquanHintCategory getCategory() {
        return IquanHintCategory.CAT_LOCAL_PARALLEL;
    }

    @Override
    public List<HintPredicate> getPrecedingPredicates() {
        return precedingPredicates;
    }

    @Override
    public boolean doApply(RelHint relHint, RelNode relNode) {
        TableScan scan = (TableScan) relNode;

        Map<String, String> kvMap = relHint.kvOptions;
        String tableName = kvMap.get(ConstantDefine.HINT_TABLE_NAME_KEY);
        if (tableName == null || tableName.trim().isEmpty()) {
            return false;
        }

        List<String> actualTablePath = scan.getTable().getQualifiedName();
        if (actualTablePath.size() != 3) {
            //todo:
            throw new RuntimeException();
        }

        IquanConfigManager conf = IquanRelOptUtils.getConfigFromRel(relNode);
        String defaultCatalogName = conf.getString(SqlConfigOptions.IQUAN_DEFAULT_CATALOG_NAME);
        String defaultDbName = conf.getString(SqlConfigOptions.IQUAN_DEFAULT_DB_NAME);

        List<String> hintTablePath = IquanHintOptUtils.getTablePath(defaultCatalogName, defaultDbName, tableName);
        return Objects.equals(actualTablePath, hintTablePath);
    }
}
