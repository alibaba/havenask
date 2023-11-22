package com.taobao.search.iquan.core.rel.visitor.relshuttle;

import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.rel.ops.logical.CTEConsumer;
import com.taobao.search.iquan.core.rel.ops.logical.CTEProducer;
import com.taobao.search.iquan.core.rel.ops.logical.LayerTable.LogicalFuseLayerTableScan;
import com.taobao.search.iquan.core.rel.ops.logical.LayerTable.LogicalLayerTableScan;
import com.taobao.search.iquan.core.rel.ops.physical.ExecCorrelateOp;
import com.taobao.search.iquan.core.rel.ops.physical.ExecLookupJoinOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanAggregateOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanCalcOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanCorrelateOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanExchangeOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanHashJoinOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanIdentityOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanJoinOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanLeftMultiJoinOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanMatchOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanMergeOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanMultiJoinOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanNestedLoopJoinOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanRelNode;
import com.taobao.search.iquan.core.rel.ops.physical.IquanSinkOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanSortOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableFunctionScanOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableModifyOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableScanOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanUncollectOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanUnionOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanValuesOp;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalTableScan;

public abstract class IquanRelShuttle implements RelShuttle {

    abstract protected RelNode visit(IquanAggregateOp rel);

    abstract protected RelNode visit(IquanCalcOp rel);

    abstract protected RelNode visit(IquanCorrelateOp rel);

    abstract protected RelNode visit(IquanExchangeOp rel);

    abstract protected RelNode visit(IquanJoinOp rel);

    abstract protected RelNode visit(IquanSinkOp rel);

    abstract protected RelNode visit(IquanSortOp rel);

    abstract protected RelNode visit(IquanTableFunctionScanOp rel);

    abstract protected RelNode visit(IquanTableScanOp rel);

    abstract protected RelNode visit(IquanUncollectOp rel);

    abstract protected RelNode visit(IquanUnionOp rel);

    abstract protected RelNode visit(IquanMergeOp rel);

    abstract protected RelNode visit(IquanValuesOp rel);

    abstract protected RelNode visit(IquanMatchOp rel);

    abstract protected RelNode visit(IquanHashJoinOp rel);

    abstract protected RelNode visit(IquanNestedLoopJoinOp rel);

    abstract protected RelNode visit(ExecLookupJoinOp rel);

    abstract protected RelNode visit(IquanLeftMultiJoinOp rel);

    abstract protected RelNode visit(IquanMultiJoinOp rel);

    abstract protected RelNode visit(IquanIdentityOp rel);

    protected RelNode visit(IquanTableModifyOp rel) {
        return rel;
    }

    protected RelNode visit(LogicalTableScan rel) {
        return rel;
    }

    protected RelNode visit(LogicalLayerTableScan rel) {
        return rel;
    }

    protected RelNode visit(LogicalFuseLayerTableScan rel) {
        return rel;
    }

    protected RelNode visit(CTEProducer rel) {
        return rel;
    }

    protected RelNode visit(CTEConsumer rel) {
        return rel;
    }

    public RelNode visit(LogicalTableModify rel) {
        return rel;
    }

    public RelNode visit(LogicalCorrelate rel) {
        return rel;
    }

    public RelNode visit(Uncollect rel) {
        return rel;
    }

    public RelNode visit(LogicalIntersect rel) {
        return rel;
    }

    abstract protected RelNode visit(ExecCorrelateOp rel);

    private RelNode visitIquanNode(IquanRelNode rel) {
        // execute ops
        if (rel instanceof IquanHashJoinOp) {
            return visit((IquanHashJoinOp) rel);
        } else if (rel instanceof IquanNestedLoopJoinOp) {
            return visit((IquanNestedLoopJoinOp) rel);
        } else if (rel instanceof ExecLookupJoinOp) {
            return visit((ExecLookupJoinOp) rel);
        } else if (rel instanceof ExecCorrelateOp) {
            return visit((ExecCorrelateOp) rel);
        }

        // physical ops
        if (rel instanceof IquanAggregateOp) {
            return visit((IquanAggregateOp) rel);
        } else if (rel instanceof IquanCalcOp) {
            return visit((IquanCalcOp) rel);
        } else if (rel instanceof IquanCorrelateOp) {
            return visit((IquanCorrelateOp) rel);
        } else if (rel instanceof IquanExchangeOp) {
            return visit((IquanExchangeOp) rel);
        } else if (rel instanceof IquanJoinOp) {
            return visit((IquanJoinOp) rel);
        } else if (rel instanceof IquanSinkOp) {
            return visit((IquanSinkOp) rel);
        } else if (rel instanceof IquanSortOp) {
            return visit((IquanSortOp) rel);
        } else if (rel instanceof IquanTableFunctionScanOp) {
            return visit((IquanTableFunctionScanOp) rel);
        } else if (rel instanceof IquanTableScanOp) {
            return visit((IquanTableScanOp) rel);
        } else if (rel instanceof IquanUncollectOp) {
            return visit((IquanUncollectOp) rel);
        } else if (rel instanceof IquanUnionOp) {
            return visit((IquanUnionOp) rel);
        } else if (rel instanceof IquanMergeOp) {
            return visit((IquanMergeOp) rel);
        } else if (rel instanceof IquanValuesOp) {
            return visit((IquanValuesOp) rel);
        } else if (rel instanceof IquanMatchOp) {
            return visit((IquanMatchOp) rel);
        } else if (rel instanceof IquanLeftMultiJoinOp) {
            return visit((IquanLeftMultiJoinOp) rel);
        } else if (rel instanceof IquanMultiJoinOp) {
            return visit((IquanMultiJoinOp) rel);
        } else if (rel instanceof IquanTableModifyOp) {
            return visit((IquanTableModifyOp) rel);
        } else if (rel instanceof IquanIdentityOp) {
            return visit((IquanIdentityOp) rel);
        }
        throw new SqlQueryException(IquanErrorCode.IQUAN_EC_DYNAMIC_PARAMS_UNSUPPORT_NODE_TYPE, rel.getDescription());
    }

    @Override
    final public RelNode visit(RelNode other) {
        if (other instanceof IquanRelNode) {
            return visitIquanNode((IquanRelNode) other);
        }
        if (other instanceof LogicalLayerTableScan) {
            return visit((LogicalLayerTableScan) other);
        } else if (other instanceof LogicalFuseLayerTableScan) {
            return visit((LogicalFuseLayerTableScan) other);
        } else if (other instanceof CTEConsumer) {
            return visit((CTEConsumer) other);
        } else if (other instanceof CTEProducer) {
            return visit((CTEProducer) other);
        } else if (other instanceof LogicalTableModify) {
            return visit((LogicalTableModify) other);
        } else if (other instanceof LogicalCorrelate) {
            return visit((LogicalCorrelate) other);
        } else if (other instanceof Uncollect) {
            return visit((Uncollect) other);
        } else if (other instanceof LogicalMatch) {
            return visit((LogicalMatch) other);
        } else if (other instanceof LogicalIntersect) {
            return visit((LogicalIntersect) other);
        }
        throw new SqlQueryException(IquanErrorCode.IQUAN_EC_DYNAMIC_PARAMS_UNSUPPORT_NODE_TYPE, other.getDescription());
    }
}
