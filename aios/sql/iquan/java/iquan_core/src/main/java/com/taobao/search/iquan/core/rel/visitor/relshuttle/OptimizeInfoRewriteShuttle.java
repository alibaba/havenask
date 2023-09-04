package com.taobao.search.iquan.core.rel.visitor.relshuttle;

import com.taobao.search.iquan.core.rel.visitor.rexshuttle.RexOptimizeInfoRewriteShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class OptimizeInfoRewriteShuttle extends IquanRelShuttleImpl {
    private static final Logger logger = LoggerFactory.getLogger(OptimizeInfoRewriteShuttle.class);

    private final RexOptimizeInfoRewriteShuttle rexShuttle;

    public OptimizeInfoRewriteShuttle(RexOptimizeInfoRewriteShuttle rexShuttle) {
        this.rexShuttle = rexShuttle;
    }

    public static List<RelNode> go(RexBuilder rexBuilder, List<RelNode> roots) {
        RexOptimizeInfoRewriteShuttle rexShuttle = new RexOptimizeInfoRewriteShuttle(rexBuilder);
        OptimizeInfoRewriteShuttle relShuttle = new OptimizeInfoRewriteShuttle(rexShuttle);
        List<RelNode> newRoots = new ArrayList<>(roots.size());

        for (RelNode root : roots) {
            newRoots.add(root.accept(relShuttle));
        }
        return newRoots;
    }

    @Override
    protected RelNode visitRex(RelNode rel) {
        return rel.accept(rexShuttle);
    }
}
