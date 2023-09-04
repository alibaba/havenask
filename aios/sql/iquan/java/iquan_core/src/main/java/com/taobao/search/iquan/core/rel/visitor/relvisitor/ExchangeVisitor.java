package com.taobao.search.iquan.core.rel.visitor.relvisitor;

import com.taobao.search.iquan.core.rel.ops.physical.IquanExchangeOp;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;

public class ExchangeVisitor extends RelVisitor {
    private final boolean needAccurateCount;
    private int exchangeCount;

    public ExchangeVisitor() {
        needAccurateCount = false;
        exchangeCount = 0;
    }

    public ExchangeVisitor(boolean needAccurateCount) {
        this.needAccurateCount = needAccurateCount;
        exchangeCount = 0;
    }

    @Override
    public void visit(RelNode node, int ordinal, RelNode parent) {
        node = IquanRelOptUtils.toRel(node);
        if (node instanceof IquanExchangeOp) {
            ++exchangeCount;
            if (!needAccurateCount) {
                return;
            }
        }
        super.visit(node, ordinal, parent);
    }

    public boolean existExchange() {
        return exchangeCount > 0;
    }

    public int getExchangeCount() {
        return exchangeCount;
    }
}
