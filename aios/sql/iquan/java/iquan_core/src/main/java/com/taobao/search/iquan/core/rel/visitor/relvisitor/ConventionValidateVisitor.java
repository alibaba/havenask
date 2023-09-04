package com.taobao.search.iquan.core.rel.visitor.relvisitor;

import com.taobao.search.iquan.core.rel.convention.IquanConvention;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;

public class ConventionValidateVisitor extends RelVisitor {
    private boolean valid;

    public ConventionValidateVisitor() {
        this.valid = true;
    }

    public boolean isValid() {
        return this.valid;
    }

    @Override
    public void visit(RelNode node, int ordinal, RelNode parent) {
        node = IquanRelOptUtils.toRel(node);
        if (node.getConvention() != IquanConvention.PHYSICAL) {
            this.valid = false;
            return;
        }
        super.visit(node, ordinal, parent);
    }
}
