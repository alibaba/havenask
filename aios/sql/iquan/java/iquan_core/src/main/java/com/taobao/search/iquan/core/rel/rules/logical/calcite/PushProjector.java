package com.taobao.search.iquan.core.rel.rules.logical.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.tools.RelBuilder;


public class PushProjector {
    protected final Project origProj;
    protected final RelNode pushedNode;
    protected final RelBuilder relBuilder;
    protected final RexBuilder rexBuilder;

    public PushProjector(Project origProj, RelNode pushedNode, RelBuilder relBuilder) {
        this.origProj = origProj;
        this.pushedNode = pushedNode;
        this.relBuilder = relBuilder;
        this.rexBuilder = relBuilder.getRexBuilder();
    }
}
