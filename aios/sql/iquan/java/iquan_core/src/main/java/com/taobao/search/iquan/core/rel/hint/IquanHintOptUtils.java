package com.taobao.search.iquan.core.rel.hint;

import com.google.common.collect.ImmutableList;
import com.taobao.search.iquan.core.common.ConstantDefine;
import org.javatuples.Pair;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IquanHintOptUtils {
    public static Pair<RelHint, Boolean> resolveHints(RelNode node1, RelNode node2, IquanHintCategory catetory) {
        if (node1 == null || node2 == null) {
            return new Pair<>(null, true);
        }

        RelHint hint1 = resolveHints(node1, catetory);
        RelHint hint2 = resolveHints(node2, catetory);

        if (hint1 == null) {
            return new Pair<>(hint2, false);
        } else if (hint2 == null) {
            return new Pair<>(hint1, true);
        } else {
            return resolveHints(hint1, hint2);
        }
    }

    public static RelHint resolveHints(RelNode node, IquanHintCategory category) {
        if (!(node instanceof Hintable)) {
            return null;
        }
        List<RelHint> hints = getHintsByCategory(((Hintable) node).getHints(), category);
        List<RelHint> sortHints = hints.stream()
                .sorted(Comparator.comparingInt(o -> o.inheritPath.size()))
                .collect(Collectors.toList());
        if (sortHints.isEmpty()) {
            return null;
        }

        RelHint bestHint = sortHints.get(0);
        if (sortHints.size() > 1) {
            RelHint nextHint = sortHints.get(1);
            if (bestHint.inheritPath.size() == nextHint.inheritPath.size()) {
                return null;
            }
        }
        return bestHint;
    }

    public static Pair<RelHint, Boolean> resolveHints(RelHint hint1, RelHint hint2) {
        if (hint1.hintName.equals(hint2.hintName)
            && hint1.listOptions.equals(hint2.listOptions)
            && hint1.kvOptions.equals(hint2.kvOptions))
        {
            return new Pair<>(hint1, true);
        }
        return new Pair<>(null, true);
    }

    public static List<RelHint> getHintsByCategory(List<RelHint> hints, IquanHintCategory category) {
        List<RelHint> relHints = new ArrayList<>();
        Map<String, IquanHint> name2IquanHint = IquanHintInitializer.iquanHintTable;
        for (RelHint hint : hints) {
            if (name2IquanHint.containsKey(hint.hintName)) {
                IquanHint iquanHint = name2IquanHint.get(hint.hintName);
                if (iquanHint.getCategory() == category) {
                    relHints.add(hint);
                }
            }
        }
        return relHints;
    }

    public static List<String> getTablePath(String defaultCatalogName, String defaultDbName, String tablePathStr) {
        String[] tablePath = tablePathStr.split(ConstantDefine.DOT_REGEX);
        if (tablePath.length == 0) {
            return ImmutableList.of();
        }

        String catalogName = defaultCatalogName;
        String dbName = defaultDbName;
        String tableName = tablePath[tablePath.length - 1];
        if (tablePath.length > 1) {
            dbName = tablePath[tablePath.length - 2];
        }

        if (tablePath.length > 2) {
            catalogName = tablePath[tablePath.length - 3];
        }

        return ImmutableList.of(catalogName, dbName, tableName);
    }
}
