package com.taobao.search.iquan.core.rel.hint;

public enum IquanHintCategory {
    CAT_JOIN("cat_join"),
    CAT_INDEX("cat_index"),
    CAT_AGG("cat_agg"),
    CAT_LOCAL_PARALLEL("cat_local_parallel"),
    CAT_SCAN_ATTR("cat_scan_attr"),
    CAT_JOIN_ATTR("cat_join_attr"),
    CAT_AGG_ATTR("cat_agg_attr"),
    CAT_CTE_ATTR("cat_cte_attr"),
    CAT_INVALID("cat_invalid");

    private final String name;

    IquanHintCategory(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
