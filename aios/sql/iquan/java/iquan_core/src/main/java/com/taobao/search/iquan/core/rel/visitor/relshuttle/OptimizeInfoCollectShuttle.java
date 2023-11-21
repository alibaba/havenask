package com.taobao.search.iquan.core.rel.visitor.relshuttle;

import java.util.ArrayList;
import java.util.List;

import com.taobao.search.iquan.core.rel.ops.physical.IquanRelNode;
import com.taobao.search.iquan.core.rel.visitor.rexshuttle.RexOptimizeInfoCollectShuttle;
import org.apache.calcite.rel.RelNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OptimizeInfoCollectShuttle extends IquanRelShuttleImpl {
    private static final Logger logger = LoggerFactory.getLogger(OptimizeInfoCollectShuttle.class);

    private final RexOptimizeInfoCollectShuttle rexShuttle;

    public OptimizeInfoCollectShuttle(RexOptimizeInfoCollectShuttle rexShuttle) {
        this.rexShuttle = rexShuttle;
    }

    public static List<RelNode> go(List<RelNode> roots) {
        RexOptimizeInfoCollectShuttle rexShuttle = new RexOptimizeInfoCollectShuttle();
        OptimizeInfoCollectShuttle relShuttle = new OptimizeInfoCollectShuttle(rexShuttle);
        List<RelNode> newRoots = new ArrayList<>();

        for (RelNode root : roots) {
            newRoots.add(root.accept(relShuttle));
        }
        return newRoots;
    }

    @Override
    protected RelNode visitRex(RelNode rel) {
        if (rel instanceof IquanRelNode) {
            ((IquanRelNode) rel).acceptForTraverse(rexShuttle);
        }
        return rel;
    }
}
