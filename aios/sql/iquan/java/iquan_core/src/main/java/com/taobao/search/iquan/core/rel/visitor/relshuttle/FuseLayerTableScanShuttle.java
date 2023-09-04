package com.taobao.search.iquan.core.rel.visitor.relshuttle;

import com.taobao.search.iquan.core.rel.ops.logical.CTEProducer;
import com.taobao.search.iquan.core.rel.ops.logical.LayerTable.LogicalFuseLayerTableScan;
import com.taobao.search.iquan.core.rel.ops.logical.LayerTable.LogicalLayerTableScan;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.logical.*;

import java.util.*;

public class FuseLayerTableScanShuttle extends RelShuttleImpl {
    private boolean found = false;

    public static RelNode go(RelNode rel) {
        FuseLayerTableScanShuttle chain = new FuseLayerTableScanShuttle();
        RelNode newRel = rel.accept(chain);
        if (chain.found) {
            return LogicalFuseLayerTableScan.createFromRelNode(newRel);
        }
        return newRel;
    }

    private RelNode visitTruncNode(RelNode rel) {
        boolean changed = false;
        List<RelNode> children = new ArrayList<>(rel.getInputs());
        for (Ord<RelNode> input : Ord.zip(rel.getInputs())) {
            RelNode child = input.e.accept(this);
            if (found) {
                child = LogicalFuseLayerTableScan.createFromRelNode(child);
                found = false;
            }
            if (child != input.e) {
                children.set(input.i, child);
                changed = true;
            }
        }
        return changed ? rel.copy(rel.getTraitSet(), children) : rel;
    }

    @Override
    public RelNode visit(LogicalJoin join) {
        return visitTruncNode(join);
    }

    @Override
    public RelNode visit(LogicalCorrelate correlate) {
        return visitTruncNode(correlate);
    }

    @Override
    public RelNode visit(LogicalUnion union) {
        return visitTruncNode(union);
    }

    @Override
    public RelNode visit(LogicalIntersect intersect) {
        return visitTruncNode(intersect);
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
        return visitTruncNode(aggregate);
    }

    @Override
    public RelNode visit(LogicalSort sort) {
        return visitTruncNode(sort);
    }

    public RelNode visit(CTEProducer producer) {
        RelNode newRel = visitChildren(producer);
        if (found) {
            RelNode fuseScan = LogicalFuseLayerTableScan.createFromRelNode(newRel.getInput(0));
            found = false;
            return newRel.copy(newRel.getTraitSet(), Collections.singletonList(fuseScan));
        }
        return newRel;
    }

    public RelNode visit(LogicalLayerTableScan scan) {
        found = true;
        return scan;
    }

    @Override
    public RelNode visit(RelNode other) {
        if (other instanceof CTEProducer) {
            return visit((CTEProducer) other);
        } else if (other instanceof LogicalLayerTableScan) {
            return visit((LogicalLayerTableScan) other);
        }
        return visitChildren(other);
    }
}