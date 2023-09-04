package com.taobao.search.iquan.core.rel.ops.physical;

public enum Scope {
    PARTIAL("PARTIAL"),
    FINAL("FINAL"),
    NORMAL("NORMAL");

    Scope(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    private String name;
}
