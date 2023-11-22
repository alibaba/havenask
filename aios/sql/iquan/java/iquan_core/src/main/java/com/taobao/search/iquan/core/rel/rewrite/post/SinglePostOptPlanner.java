package com.taobao.search.iquan.core.rel.rewrite.post;

import java.util.ArrayList;
import java.util.List;

import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.rel.ops.physical.IquanAggregateOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanExchangeOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanSinkOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanSortOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableFunctionScanOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanUnionOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanValuesOp;
import com.taobao.search.iquan.core.rel.ops.physical.Scope;
import com.taobao.search.iquan.core.rel.visitor.relshuttle.ParallelOptimizeShuttle;
import com.taobao.search.iquan.core.rel.visitor.relvisitor.ConventionValidateVisitor;
import com.taobao.search.iquan.core.rel.visitor.relvisitor.ExchangeVisitor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinglePostOptPlanner {
    private static final Logger logger = LoggerFactory.getLogger(PostOptPlanner.class);

    public List<RelNode> optimize(List<RelNode> roots) {
        List<RelNode> newRoots = new ArrayList<>();
        for (RelNode root : roots) {
            newRoots.add(optimize(root));
        }
        return newRoots;
    }

    private RelNode optimize(RelNode root) {
        checkConvention(root);
        if (checkEmptyValues(root)) {
            return addSink(root);
        }
        root = checkAndRemoveExchangeIfExist(root);
        if (checkExchange(root)) {
            //error:must have no exchange
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_WORKFLOW_CONVERT_FAIL,
                    "RelNode optimize fail, node must have no exchange.");
        }
        root = ParallelOptimizeShuttle.go(root);
        return addSink(root);
    }

    private void checkConvention(RelNode node) {
        ConventionValidateVisitor visitor = new ConventionValidateVisitor();
        visitor.go(node);
        if (!visitor.isValid()) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_WORKFLOW_CONVERT_FAIL,
                    "RelNode optimize fail, SQL is not support.");
        }
    }

    private RelNode checkAndRemoveExchangeIfExist(RelNode node) {
        if (node instanceof Exchange) {
            return checkAndRemoveExchangeIfExist(((Exchange) node).getInput());
        }
        node = removeExchange(node);
        List<RelNode> newInputs = new ArrayList<>();
        boolean changed = false;
        for (RelNode input : node.getInputs()) {
            RelNode newInput = checkAndRemoveExchangeIfExist(input);
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

    private RelNode removeExchange(RelNode node) {
        if (node instanceof IquanAggregateOp) {
            node = removeAggregateExchange((IquanAggregateOp) node);
        }
        if (node instanceof IquanSortOp) {
            node = removeSortExchange((IquanSortOp) node);
        }
        if (node instanceof IquanUnionOp) {
            node = removeUnionExchange((IquanUnionOp) node);
        }
        if (node instanceof IquanTableFunctionScanOp) {
            node = removeTableFunctionScanExchange((IquanTableFunctionScanOp) node);
        }
        return node;
    }

    private RelNode removeAggregateExchange(IquanAggregateOp node) {
        Scope scope = node.getScope();
        if (scope.equals(Scope.NORMAL)) {
            return node;
        }
        if (scope.equals(Scope.PARTIAL)) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_WORKFLOW_CONVERT_FAIL,
                    "AggregateOp remove exchange fail, scope is partial.");
        }
        IquanExchangeOp iquanExchangeOp;
        IquanAggregateOp iquanAggregateOp;
        RelNode input1 = node.getInput();
        if (input1 instanceof IquanExchangeOp) {
            iquanExchangeOp = (IquanExchangeOp) input1;
        } else {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_WORKFLOW_CONVERT_FAIL,
                    "AggregateOp remove exchange fail, input1 is not exchange.");
        }
        RelNode input2 = iquanExchangeOp.getInput();
        if (input2 instanceof IquanAggregateOp) {
            iquanAggregateOp = (IquanAggregateOp) input2;
        } else {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_WORKFLOW_CONVERT_FAIL,
                    "AggregateOp remove exchange fail, input2 is not aggregate.");
        }
        return new IquanAggregateOp(
                node.getCluster(),
                node.getTraitSet(),
                node.getHints(),
                iquanAggregateOp.getInput(),
                node.getAggCalls(),
                node.getGroupKeyFieldList(),
                iquanAggregateOp.getInputParams(),
                node.getOutputParams(),
                node.deriveRowType(),
                Scope.NORMAL
        );
    }

    private RelNode removeSortExchange(IquanSortOp node) {
        IquanExchangeOp iquanExchangeOp;
        IquanSortOp iquanSortOp;
        RelNode input1 = node.getInput();
        if (input1 instanceof IquanExchangeOp) {
            iquanExchangeOp = (IquanExchangeOp) input1;
        } else {
            //exchange removed by SortExchangeRemoveRule
            return node;
        }
        RelNode input2 = iquanExchangeOp.getInput();
        if (input2 instanceof IquanSortOp) {
            iquanSortOp = (IquanSortOp) input2;
        } else {
            //sortOp removed by LimitPushDownRule
            return new IquanSortOp(
                    node.getCluster(),
                    node.getTraitSet(),
                    iquanExchangeOp.getInput(),
                    node.getCollation(),
                    node.offset,
                    node.fetch);
        }
        return new IquanSortOp(
                node.getCluster(),
                node.getTraitSet(),
                iquanSortOp.getInput(),
                node.getCollation(),
                node.offset,
                node.fetch);
    }

    private RelNode removeTableFunctionScanExchange(IquanTableFunctionScanOp node) {
        Scope scope = node.getScope();
        if (scope.equals(Scope.NORMAL)) {
            return node;
        }
        if (scope.equals(Scope.PARTIAL)) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_WORKFLOW_CONVERT_FAIL,
                    "TableFunctionScan remove exchange fail, scope is partial.");
        }
        IquanExchangeOp iquanExchangeOp;
        IquanTableFunctionScanOp iquanTableFunctionScanOp;
        List<RelNode> inputs1 = node.getInputs();
        if ((inputs1.size() == 1) && (inputs1.get(0) instanceof IquanExchangeOp)) {
            iquanExchangeOp = (IquanExchangeOp) inputs1.get(0);
        } else {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_WORKFLOW_CONVERT_FAIL,
                    "TableFunctionScan remove exchange fail, inputs1 is not single exchange.");
        }
        RelNode input2 = iquanExchangeOp.getInput();
        if (input2 instanceof IquanTableFunctionScanOp) {
            iquanTableFunctionScanOp = (IquanTableFunctionScanOp) input2;
        } else {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_WORKFLOW_CONVERT_FAIL,
                    "TableFunctionScan remove exchange fail, input2 is not TableFunctionScan.");
        }
        return new IquanTableFunctionScanOp(
                node.getCluster(),
                node.getTraitSet(),
                iquanTableFunctionScanOp.getInputs(),
                node.getCall(),
                node.getElementType(),
                node.getRowType(),
                node.getColumnMappings(),
                Scope.NORMAL,
                node.isBlock(),
                node.isEnableShuffle()
        );
    }

    private RelNode removeUnionExchange(IquanUnionOp node) {
        if (node.all) {
            return node;
        }
        IquanExchangeOp iquanExchangeOp;
        IquanUnionOp iquanUnionOp;
        List<RelNode> inputs1 = node.getInputs();
        if ((inputs1.size() == 1) && (inputs1.get(0) instanceof IquanExchangeOp)) {
            iquanExchangeOp = (IquanExchangeOp) inputs1.get(0);
        } else {
            //exchange removed UnionExchangeRemoveRule
            return node;
        }
        RelNode input2 = iquanExchangeOp.getInput();
        if (input2 instanceof IquanUnionOp) {
            iquanUnionOp = (IquanUnionOp) input2;
        } else {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_SQL_WORKFLOW_CONVERT_FAIL,
                    "UnionOp remove exchange fail, input2 is not union.");
        }
        return new IquanUnionOp(
                node.getCluster(),
                node.getTraitSet(),
                iquanUnionOp.getInputs(),
                node.all,
                new ArrayList<>()
        );
    }

    private boolean checkExchange(RelNode node) {
        ExchangeVisitor visitor = new ExchangeVisitor();
        visitor.go(node);
        return visitor.existExchange();
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

    private RelNode addSink(RelNode node) {
        return new IquanSinkOp(
                node.getCluster(),
                node.getTraitSet(),
                node
        );
    }
}
