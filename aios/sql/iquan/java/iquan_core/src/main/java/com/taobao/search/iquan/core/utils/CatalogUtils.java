package com.taobao.search.iquan.core.utils;

import com.taobao.search.iquan.client.common.json.common.JsonIndex;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.api.schema.Table;
import com.taobao.search.iquan.core.rel.ops.logical.LayerTable.LogicalLayerTableScan;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CatalogUtils {
    private static final Logger logger = LoggerFactory.getLogger(CatalogUtils.class);

    private CatalogUtils() {
    }

    public static List<String> getTablePath(RelNode node) {
        if (node instanceof TableScan) {
            return ((TableScan) node).getTable().getQualifiedName();
        } else if (node instanceof LogicalLayerTableScan) {
            return ((LogicalLayerTableScan) node).getQualifiedName();
        }
        throw new SqlQueryException(IquanErrorCode.IQUAN_EC_INTERNAL_ERROR,
                "only scan node have table path");
    }

    public static List<JsonIndex> getIndex(Table table, List<String> fieldNames, String indexType, Map<String, String> extendInfos) {
        assert !fieldNames.isEmpty();
        assert table.getJsonTable() != null;
        assert table.getJsonTable().getConstraint() != null;
        assert table.getJsonTable().getConstraint().getMultiIndexes() != null;

        List<JsonIndex> indexList = new ArrayList<>();
        for (JsonIndex jsonIndex : table.getJsonTable().getConstraint().getMultiIndexes()) {
            if (jsonIndex.getIndexFields().equals(fieldNames)) {
                if (indexType != null && !jsonIndex.getIndexType().equalsIgnoreCase(indexType)) {
                    logger.debug(String.format("table[%s] expect indexType[%s] but acutal is [%s]",
                        table.getTableName(), indexType, jsonIndex.getIndexType()));
                    continue;
                }
                if (extendInfos != null && jsonIndex.getExtendInfos() != null) {
                    boolean matched = jsonIndex.getExtendInfos().entrySet().containsAll(extendInfos.entrySet());
                    if (!matched) {
                        logger.debug(String.format("table[%s] expect extend[%s] but acutal is [%s]",
                            table.getTableName(), extendInfos.toString(), jsonIndex.getExtendInfos().toString()));
                        continue;
                    }
                }
                indexList.add(jsonIndex);
            }
        }
        return indexList;
    }
}
