package com.taobao.search.iquan.core.rel.ops.logical;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.hint.RelHint;

public class CTEProducer extends SingleRel {

    private final String name;
    private final int index;
    private final List<RelHint> hints;

    public CTEProducer(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            String name,
            int index,
            List<RelHint> hints) {
        super(cluster, traits, input);
        this.name = name;
        this.index = index;
        this.hints = hints;
    }

    public String getName() {
        return name;
    }

    public int getIndex() {
        return index;
    }

    public List<RelHint> getHints() {
        return hints;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new CTEProducer(getCluster(), traitSet, inputs.get(0), name, index, hints);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.item("name", name);
        pw.item("index", index);
        return pw;
    }
}
