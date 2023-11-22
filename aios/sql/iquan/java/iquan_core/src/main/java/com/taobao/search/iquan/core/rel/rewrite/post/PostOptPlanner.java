package com.taobao.search.iquan.core.rel.rewrite.post;

import java.util.ArrayList;
import java.util.List;

import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.config.IquanConfigManager;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.catalog.GlobalCatalog;
import com.taobao.search.iquan.core.rel.ops.physical.IquanRelNode;
import com.taobao.search.iquan.core.rel.ops.physical.IquanSinkOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableModifyOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanValuesOp;
import com.taobao.search.iquan.core.rel.visitor.relshuttle.IquanDistributeShuttle;
import com.taobao.search.iquan.core.rel.visitor.relshuttle.ParallelOptimizeShuttle;
import com.taobao.search.iquan.core.rel.visitor.relvisitor.ConventionValidateVisitor;
import org.apache.calcite.rel.RelNode;

public class PostOptPlanner {
    //private static final Logger logger = LoggerFactory.getLogger(PostOptPlanner.class);

    public List<RelNode> optimize(List<RelNode> roots, GlobalCatalog catalog, String dbName, IquanConfigManager config) {

        List<RelNode> newRoots = new ArrayList<>();
        for (RelNode root : roots) {
            newRoots.add(optimize(root, catalog, dbName, config));
        }
        return newRoots;
    }

    private RelNode optimize(RelNode root, GlobalCatalog catalog, String dbName, IquanConfigManager config) {
        checkConvention(root);
        if (root instanceof IquanTableModifyOp) {
            return root;
        }

        //root = checkAndAddExchangeIfNotExist(root);
        root = IquanDistributeShuttle.go(root, catalog, config);
        if (checkEmptyValues(root)) {
            return new IquanSinkOp(
                    root.getCluster(),
                    root.getTraitSet(),
                    root
            );
        }

        root = ParallelOptimizeShuttle.go(root);

        return addSink(root, catalog, config);
    }

    private void checkConvention(RelNode node) {
        ConventionValidateVisitor visitor = new ConventionValidateVisitor();
        visitor.go(node);
        if (!visitor.isValid()) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_WORKFLOW_CONVERT_FAIL,
                    "RelNode optimize fail, SQL is not support.");
        }
    }

    private boolean checkEmptyValues(RelNode node) {
        if (node instanceof IquanValuesOp) {
            IquanValuesOp values = (IquanValuesOp) node;
            if (!values.getTuples().isEmpty()) {
                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_VALUES_NOT_EMPTY_VALUES,
                        String.format("size of tuple is %d", values.getTuples()));
            }
            return true;
        }
        return false;
    }

    /*
    private boolean checkExchange(RelNode node) {
        ExchangeVisitor visitor = new ExchangeVisitor();
        visitor.go(node);
        return visitor.existExchange();
    }

    private RelNode checkAndAddExchangeIfNotExist(RelNode node) {
        if (node instanceof Exchange) {
            return node;
        }
        if (!checkExchange(node)) {
            return addExchange(node);
        }

        List<RelNode> newInputs = new ArrayList<>();
        boolean changed = false;
        for (RelNode input : node.getInputs()) {
            RelNode newInput = checkAndAddExchangeIfNotExist(input);
            if (newInput != input) {
                changed = true;
            }
            newInputs.add(newInput);
        }
        if (changed) {
            return node.copy(node.getTraitSet(), newInputs);
        }
        return node;
    }

    private RelNode addExchange(RelNode node) {
        return new IquanExchangeOp(
                node.getCluster(),
                node.getTraitSet(),
                node,
                Distribution.SINGLETON
        );
    }
    */

    private RelNode addSink(RelNode node, GlobalCatalog catalog, IquanConfigManager config) {
        IquanRelNode iquanRelNode = new IquanSinkOp(
                node.getCluster(),
                node.getTraitSet(),
                node
        );
        return iquanRelNode.deriveDistribution(iquanRelNode.getInputs(), catalog, config);
    }
}
