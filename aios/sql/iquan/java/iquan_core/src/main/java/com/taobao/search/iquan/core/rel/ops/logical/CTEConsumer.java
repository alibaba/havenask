package com.taobao.search.iquan.core.rel.ops.logical;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;

public class CTEConsumer extends AbstractRelNode {

    private final CTEProducer producer;

    public CTEConsumer(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            CTEProducer producer) {
        super(cluster, traitSet);
        this.producer = producer;
    }

    public CTEProducer getProducer() {
        return producer;
    }

    @Override
    protected RelDataType deriveRowType() {
        return producer.getRowType();
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new CTEConsumer(getCluster(), traitSet, producer);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.item("index", producer.getIndex());
        return pw;
    }
}
