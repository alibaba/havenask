package com.taobao.search.iquan.core.api.schema;

public class SortDesc {
    private String field;
    private String order;

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getOrder() {
        return order;
    }

    public void setOrder(String order) {
        this.order = order;
    }

    public SortDesc(String field, String order) {
        this.field = field;
        this.order = order;
    }
}
