package com.taobao.search.iquan.core.rel;

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Util;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class IquanRelBuilder extends RelBuilder {
    private final Context context;
    protected IquanRelBuilder(Context context, RelOptCluster cluster, RelOptSchema relOptSchema) {
        super(context, cluster, relOptSchema);
        this.context = context == null ? Contexts.EMPTY_CONTEXT : context;
    }

    @Override
    public RelBuilder aggregate(GroupKey groupKey, Iterable<AggCall> aggCalls) {
        RelNode relNode = super.aggregate(groupKey, aggCalls).build();

        Function<LogicalAggregate, Boolean> isCountStartAgg = agg -> {
            if (agg.getGroupCount() != 0 || agg.getAggCallList().size() != 1) {
                return false;
            }
            AggregateCall call = agg.getAggCallList().get(0);
            return call.getAggregation().getKind() == SqlKind.COUNT &&call.filterArg == -1 && call.getArgList().isEmpty();
        };

        LogicalProject project = null;
        RelNode curRelNode = relNode;
        if (relNode instanceof LogicalProject) {
            project = (LogicalProject) relNode;
            curRelNode = project.getInput();
        }

        if (curRelNode instanceof LogicalAggregate) {
            LogicalAggregate logicalAggregate = (LogicalAggregate) curRelNode;
            if (isCountStartAgg.apply(logicalAggregate)) {
                RelNode newAggInput = push(logicalAggregate.getInput(0))
                        .project(literal(0))
                        .build();
                RelBuilder curBuilder = push(logicalAggregate.copy(logicalAggregate.getTraitSet(), Collections.singletonList(newAggInput)));
                return project == null ? curBuilder : curBuilder.projectNamed(project.getProjects(), project.getRowType().getFieldNames(), false);
            }
        }
        return push(relNode);
    }

    @Override
    public RelBuilder transform(UnaryOperator<Config> transform) {
        Config config =
                context.maybeUnwrap(Config.class).orElse(Config.DEFAULT);
        boolean simplify = Hook.REL_BUILDER_SIMPLIFY.get(config.simplify());
        config = config.withSimplify(simplify);
        RelFactories.Struct newStruct =
                requireNonNull(RelFactories.Struct.fromContext(context));
        final Context newContext =
                Contexts.of(newStruct, transform.apply(config));
        return new IquanRelBuilder(newContext, cluster, relOptSchema);
    }

    public static RelBuilderFactory proto(Context context) {
        return (cluster, schema) -> {
            Context iquanContext = cluster.getPlanner().getContext();
            return new IquanRelBuilder(Contexts.chain(context, iquanContext), cluster, schema);
        };
    }

    public static final RelBuilderFactory LOGICAL_BUILDER =
            IquanRelBuilder.proto(Contexts.EMPTY_CONTEXT);
}
