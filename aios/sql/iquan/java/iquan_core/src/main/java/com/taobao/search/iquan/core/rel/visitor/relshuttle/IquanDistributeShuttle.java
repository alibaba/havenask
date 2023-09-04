package com.taobao.search.iquan.core.rel.visitor.relshuttle;

import com.google.common.collect.ImmutableList;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.config.IquanConfigManager;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.api.schema.*;
import com.taobao.search.iquan.core.catalog.GlobalCatalog;
import com.taobao.search.iquan.core.rel.ops.physical.*;
import com.taobao.search.iquan.core.utils.RelDistributionUtil;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class IquanDistributeShuttle extends IquanRelShuttle {
    private static final Logger logger = LoggerFactory.getLogger(IquanDistributeShuttle.class);
    private final GlobalCatalog catalog;
    private final String dbName;
    private final IquanConfigManager config;

    public IquanDistributeShuttle(GlobalCatalog catalog, String dbName, IquanConfigManager config) {
        this.catalog = catalog;
        this.dbName = dbName;
        this.config = config;
    }

    public static RelNode go(RelNode root, GlobalCatalog catalog, String dbName, IquanConfigManager config) {
        IquanDistributeShuttle relShuttle = new IquanDistributeShuttle(catalog, dbName, config);
        return relShuttle.optimize(root);
    }

    private RelNode optimize(RelNode root) {
        IquanRelNode iquanRelNode = (IquanRelNode) root;
        if ((iquanRelNode.getLocation() != null) && (iquanRelNode.getOutputDistribution() != null)) {
            return root;
        }
        final List<RelNode> inputs = root.getInputs();
        if (inputs.isEmpty()) {
            return root.accept(this);
        }
        for (int i = 0; i < inputs.size(); i++) {
            RelNode newInput = optimize(inputs.get(i));
            if (newInput instanceof IquanValuesOp) {
                return newInput;
            }
            if (newInput != inputs.get(i)) {
                /**
                 * 1. cte exchanged to different stage(node1, node2 has different input)
                 * {{{
                 *      node3
                 *     /     \
                 *  node1  node2
                 *    |     |
                 *   ex1   ex2
                 *    \    /
                 *     cte
                 * }}}
                 *
                 * 2. cte exchanged to same stage (this plan not happen)
                 * {{{
                 *     node3
                 *    /     \
                 *   ex1   ex2
                 *    \    /
                 *     cte
                 * }}}
                 *
                 * 3. cte output to same stage(cte processed in first visit)
                 * {{{
                 *     node3
                 *    /     \
                 *  node1  node2
                 *    \    /
                 *     cte
                 * }}}
                 */
                root.replaceInput(i, newInput);
            }
        }
        return root.accept(this);
    }

    @Override
    protected RelNode visit(IquanAggregateOp rel) {
        return rel.deriveDistribution(rel.getInputs(), catalog, dbName, config);
    }

    @Override
    protected RelNode visit(IquanCalcOp rel) {
        return rel.deriveDistribution(rel.getInputs(), catalog, dbName, config);
    }

    @Override
    protected RelNode visit(IquanCorrelateOp rel) {
        return rel.deriveDistribution(rel.getInputs(), catalog, dbName, config);
    }

    @Override
    protected RelNode visit(IquanExchangeOp rel) {
        return null;
    }

    @Override
    protected RelNode visit(IquanJoinOp rel) {
        return null;
    }

    @Override
    protected RelNode visit(IquanSinkOp rel) {
        return rel.deriveDistribution(rel.getInputs(), catalog, dbName, config);
    }

    @Override
    protected RelNode visit(IquanSortOp rel) {
        return rel.deriveDistribution(rel.getInputs(), catalog, dbName, config);
    }

    @Override
    protected RelNode visit(IquanTableFunctionScanOp rel) {
        return rel.deriveDistribution(rel.getInputs(), catalog, dbName, config);
    }

    @Override
    protected RelNode visit(IquanTableScanOp rel) {
        return rel.deriveDistribution(null, catalog, dbName, config);
    }

    @Override
    protected RelNode visit(IquanUncollectOp rel) {
        return null;
    }

    @Override
    protected RelNode visit(IquanUnionOp rel) {
        return rel.deriveDistribution(rel.getInputs(), catalog, dbName, config);
    }

    @Override
    protected RelNode visit(IquanMergeOp rel) {
        return null;
    }

    @Override
    protected RelNode visit(IquanValuesOp rel) {
        return rel;
    }

    @Override
    protected RelNode visit(IquanMatchOp rel) {
        return rel.deriveDistribution(rel.getInputs(), catalog, dbName, config);
    }

    @Override
    protected RelNode visit(IquanHashJoinOp hashJoinOp) {
        return hashJoinOp.deriveDistribution(hashJoinOp.getInputs(), catalog, dbName, config);
    }

    @Override
    protected RelNode visit(IquanNestedLoopJoinOp loopJoinOp) {
        return loopJoinOp.deriveDistribution(loopJoinOp.getInputs(), catalog, dbName, config);
    }

    @Override
    protected RelNode visit(ExecLookupJoinOp lookupJoinOp) {
        lookupJoinOp.getBuildOp().accept(this);
        return lookupJoinOp.deriveDistribution(lookupJoinOp.getInputs(), catalog, dbName, config);
    }

    @Override
    protected RelNode visit(IquanLeftMultiJoinOp leftMultiJoinOp) {
        return leftMultiJoinOp.deriveDistribution(leftMultiJoinOp.getInputs(), catalog, dbName, config);
    }

    @Override
    protected RelNode visit(IquanMultiJoinOp rel) {
        return null;
    }

    @Override
    protected RelNode visit(IquanIdentityOp iquanIdentityOp) {
        return iquanIdentityOp.deriveDistribution(iquanIdentityOp.getInputs(), catalog, dbName, config);
    }

    @Override
    protected RelNode visit(ExecCorrelateOp rel) {
        return rel.deriveDistribution(rel.getInputs(), catalog, dbName, config);
    }

    @Override
    public RelNode visit(TableScan scan) {
        IquanTableScanBase iquanTableScanBase;
        if (scan instanceof IquanTableScanBase) {
            iquanTableScanBase = (IquanTableScanBase) scan;
        } else {
            throw new SqlQueryException(IquanErrorCode.IQUAN_FAIL,
                    String.format("not support rel node: %s", scan.toString()));
        }
        List<String> tablePath = iquanTableScanBase.getTable().getQualifiedName();
        Table table = IquanRelOptUtils.getIquanTable(iquanTableScanBase.getTable());
        Location tableLocation = table.getLocation();
        Distribution distribution = table.getDistribution().copy();
        distribution.setPartFixKeyMap(RelDistributionUtil.getPartFixKeyMap(tablePath, table));
        IquanRelNode preRel = iquanTableScanBase;
        preRel.setLocation(tableLocation);
        preRel.setOutputDistribution(distribution);
        List<IquanRelNode> pushDownOps = iquanTableScanBase.getPushDownOps();
        IquanSortOp pushDownSortOp = null;
        boolean needGlobalSort = false;
        for (int i = 0; i < pushDownOps.size(); i++) {
            IquanRelNode relNode = pushDownOps.get(i);
            if (relNode instanceof IquanSortOp) {
                assert pushDownSortOp == null;
                pushDownSortOp = (IquanSortOp) relNode;
                if (preRel.getOutputDistribution().getType() == RelDistribution.Type.SINGLETON
                        || preRel.getOutputDistribution().getType() == RelDistribution.Type.BROADCAST_DISTRIBUTED) {
                    preRel = relNode.simpleRelDerive(preRel);
                } else {
                    preRel = RelDistributionUtil.genLocalSort(pushDownSortOp, preRel);
                    pushDownOps.set(i, preRel);
                    needGlobalSort = true;
                }
            } else if (relNode instanceof IquanTableFunctionScanOp) {
                preRel = relNode.simpleRelDerive(preRel);
            } else {
                //when beyond db query, push down ops located the same with table
                preRel = relNode.deriveDistribution(ImmutableList.of(preRel), catalog, tablePath.get(tablePath.size() - 2), config);
            }
        }
        if (needGlobalSort) {
            iquanTableScanBase.setLocation(preRel.getLocation());
            iquanTableScanBase.setOutputDistribution(preRel.getOutputDistribution());
            return RelDistributionUtil.genGlobalSort(iquanTableScanBase, pushDownSortOp, catalog, dbName, config);
        }
        if (preRel != iquanTableScanBase) {
            iquanTableScanBase.setLocation(preRel.getLocation());
            iquanTableScanBase.setOutputDistribution(preRel.getOutputDistribution());
        }
        return iquanTableScanBase;
    }

    @Override
    public RelNode visit(TableFunctionScan scan) {
        if (scan instanceof IquanTableFunctionScanOp) {
            return ((IquanTableFunctionScanOp) scan).deriveDistribution(scan.getInputs(), catalog, dbName, config);
        } else {
            throw new SqlQueryException(IquanErrorCode.IQUAN_FAIL,
                    String.format("not support rel node: %s", scan.toString()));
        }
    }

    @Override
    public RelNode visit(LogicalValues values) {
        return null;
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        return null;
    }

    @Override
    public RelNode visit(LogicalCalc calc) {
        return null;
    }

    @Override
    public RelNode visit(LogicalProject project) {
        return null;
    }

    @Override
    public RelNode visit(LogicalJoin join) {
        return null;
    }

    @Override
    public RelNode visit(LogicalCorrelate correlate) {
        return null;
    }

    @Override
    public RelNode visit(LogicalUnion union) {
        return null;
    }

    @Override
    public RelNode visit(LogicalIntersect intersect) {
        return null;
    }

    @Override
    public RelNode visit(LogicalMinus minus) {
        return null;
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
        return null;
    }

    @Override
    public RelNode visit(LogicalMatch match) {
        return null;
    }

    @Override
    public RelNode visit(LogicalSort sort) {
        return null;
    }

    @Override
    public RelNode visit(LogicalExchange exchange) {
        return null;
    }

    @Override
    public RelNode visit(LogicalTableModify modify) {
        return null;
    }
}
