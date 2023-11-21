package com.taobao.search.iquan.core.rel.ops.logical.LayerTable;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

public class LayerTableDistinctFactory {
    public static LayerTableDistinct createDistinct(LogicalLayerTableScan scan) {
        if (scan == null) {
            return null;
        }
        Map<String, Object> properties = scan.getLayerTable().getProperties();
        if (properties == null || !properties.containsKey("distinct")) {
            return null;
        }
        Map<String, Object> distinct = (Map<String, Object>) properties.get("distinct");
        String distinctType = (String) distinct.get("type");
        if (StringUtils.equals(distinctType, "agg")) {
            return new LayerTableAggDistinct(scan);
        } else if (StringUtils.equals(distinctType, "rankTvf")) {
            return new LayerTableTvfDistinct(scan);
        } else {
            return null;
        }
    }
}
