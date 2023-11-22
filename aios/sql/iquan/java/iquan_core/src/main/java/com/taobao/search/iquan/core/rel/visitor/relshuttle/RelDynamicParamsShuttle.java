package com.taobao.search.iquan.core.rel.visitor.relshuttle;

import java.util.ArrayList;
import java.util.List;

import com.taobao.search.iquan.core.rel.visitor.rexshuttle.RexDynamicParamsShuttle;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelDynamicParamsShuttle extends RelDeepCopyShuttle {
    private static final Logger logger = LoggerFactory.getLogger(RelDynamicParamsShuttle.class);

    private RexDynamicParamsShuttle rexShuttle;

    protected RelDynamicParamsShuttle(RelOptCluster cluster, RexDynamicParamsShuttle rexShuttle) {
        super(cluster);
        this.rexShuttle = rexShuttle;
    }

    public static List<RelNode> go(RelOptCluster cluster, RexBuilder rexBuilder, List<List<Object>> dynamicParams, List<RelNode> roots) {
        assert dynamicParams.size() == roots.size();

        RexDynamicParamsShuttle rexShuttle = new RexDynamicParamsShuttle(rexBuilder, dynamicParams);
        RelDynamicParamsShuttle relShuttle = new RelDynamicParamsShuttle(cluster, rexShuttle);
        List<RelNode> newRoots = new ArrayList<>();

        for (int i = 0; i < roots.size(); ++i) {
            rexShuttle.setIndex(i);
            newRoots.add(
                    roots.get(i).accept(relShuttle)
            );
        }
        return newRoots;
    }

    @Override
    protected RelNode visitRex(RelNode rel) {
        return rel.accept(rexShuttle);
    }
}
