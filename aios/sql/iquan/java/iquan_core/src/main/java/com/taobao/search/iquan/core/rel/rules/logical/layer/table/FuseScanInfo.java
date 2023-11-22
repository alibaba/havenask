package com.taobao.search.iquan.core.rel.rules.logical.layer.table;

import java.util.ArrayDeque;
import java.util.Deque;

import com.taobao.search.iquan.core.rel.ops.logical.LayerTable.LogicalLayerTableScan;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;

public class FuseScanInfo {
    private final Deque<RelNode> ancestors;
    private boolean isLayerTableScan = false;
    private LogicalLayerTableScan scan = null;

    public FuseScanInfo(RelNode root) {
        ancestors = new ArrayDeque<>();
        RelVisitor visitor = new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                ancestors.add(node);
                if (node instanceof LogicalSort || node instanceof LogicalAggregate) {
                    return;
                } else if (node instanceof LogicalUnion || node instanceof LogicalJoin || node instanceof LogicalTableScan) {
                    return;
                } else if (node instanceof LogicalLayerTableScan) {
                    isLayerTableScan = true;
                    scan = (LogicalLayerTableScan) node;
                    return;
                }
                super.visit(node, ordinal, parent);
            }

            @Override
            public RelNode go(RelNode p) {
                return super.go(p);
            }
        };
        visitor.go(root);
    }

    public FuseScanInfo(Deque<RelNode> ancestors) {
        this.ancestors = ancestors;
        if (ancestors.getLast() instanceof LogicalLayerTableScan) {
            this.isLayerTableScan = true;
            this.scan = (LogicalLayerTableScan) ancestors.getLast();
        }
    }

    public Deque<RelNode> getAncestors() {
        return ancestors;
    }

    public boolean isLayerTableScan() {
        return isLayerTableScan;
    }

    public LogicalLayerTableScan getScan() {
        return scan;
    }
}
