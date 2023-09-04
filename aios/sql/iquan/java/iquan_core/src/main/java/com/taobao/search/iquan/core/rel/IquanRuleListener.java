package com.taobao.search.iquan.core.rel;

import org.apache.calcite.plan.RelOptListener;

public abstract class IquanRuleListener implements RelOptListener {
    public abstract String display();
}
