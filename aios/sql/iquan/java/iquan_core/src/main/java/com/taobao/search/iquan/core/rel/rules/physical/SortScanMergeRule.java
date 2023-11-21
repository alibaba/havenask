package com.taobao.search.iquan.core.rel.rules.physical;

import java.util.ArrayList;
import java.util.List;

import com.taobao.search.iquan.core.api.schema.IquanTable;
import com.taobao.search.iquan.core.api.schema.SortDesc;
import com.taobao.search.iquan.core.rel.IquanRelBuilder;
import com.taobao.search.iquan.core.rel.ops.physical.IquanCalcOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanRelNode;
import com.taobao.search.iquan.core.rel.ops.physical.IquanSortOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableScanBase;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableScanOp;
import com.taobao.search.iquan.core.rel.visitor.rexshuttle.RexMatchTypeShuttle;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.tools.RelBuilderFactory;

public class SortScanMergeRule extends RelOptRule {
    public static final SortScanMergeRule INSTANCE = new SortScanMergeRule(IquanRelBuilder.LOGICAL_BUILDER);

    private SortScanMergeRule(RelBuilderFactory relBuilderFactory) {
        super(operand(
                IquanSortOp.class,
                operand(
                        IquanTableScanBase.class,
                        none()
                )
        ), relBuilderFactory, null);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final IquanTableScanBase scanBase = call.rel(1);
        final IquanSortOp sort = call.rel(0);
        if (!(scanBase instanceof IquanTableScanOp)) {
            return;
        }
        IquanTableScanOp scan = (IquanTableScanOp) scanBase;
        List<IquanRelNode> pushDownOps = scan.getPushDownOps();
        if (pushDownOps.size() != 1) {
            return;
        }
        IquanRelNode node = pushDownOps.get(0);
        if (!(node instanceof IquanCalcOp)) {
            return;
        }
        IquanCalcOp calc = (IquanCalcOp) node;
        // skip matchdata
        RexMatchTypeShuttle shuttle = new RexMatchTypeShuttle();
        calc.accept(shuttle);
        if (!shuttle.getMathTypes().isEmpty()) {
            return;
        }
        IquanTable scanIquanTable = IquanRelOptUtils.getIquanTable(scan);
        List<SortDesc> tableSortDescs = scanIquanTable.getSortDescs();

        List<RelFieldCollation> opSortDescs = sort.getCollation().getFieldCollations();
        if (opSortDescs.isEmpty() || opSortDescs.size() > tableSortDescs.size()) {
            return;
        }

        for (int i = 0; i < opSortDescs.size(); ++i) {
            RelFieldCollation opSortDesc = opSortDescs.get(i);
            RexProgram rexProgram = calc.getProgram();
            RexLocalRef project = rexProgram.getProjectList().get(opSortDesc.getFieldIndex());
            RexNode expr = rexProgram.getExprList().get(project.getIndex());
            if (!(expr instanceof RexInputRef)) {
                return;
            }
            RexInputRef inputRef = (RexInputRef) expr;
            String opInputFieldName = rexProgram.getInputRowType()
                    .getFieldList()
                    .get(inputRef.getIndex())
                    .getName();
            String opOutputFieldName = rexProgram.getOutputRowType()
                    .getFieldList()
                    .get(opSortDesc.getFieldIndex())
                    .getName();

            RelFieldCollation.Direction opDirection = opSortDesc.getDirection();

            SortDesc tableSortDesc = tableSortDescs.get(i);
            if (!opInputFieldName.equals(opOutputFieldName)) {
                return;
            }
            if (!opInputFieldName.equals(tableSortDesc.getField())) {
                return;
            }

            if (!opDirection.shortString.equals(tableSortDesc.getOrder())) {
                return;
            }
        }

        List<IquanRelNode> newPushDownOps = new ArrayList<>(pushDownOps.size() + 1);
        newPushDownOps.addAll(pushDownOps);
        newPushDownOps.add(sort);
        IquanTableScanBase newScan = (IquanTableScanBase) scan.copy(scan.getUncollectOps(), newPushDownOps, scan.getLimit(), scan.isRewritable());
        call.transformTo(newScan);
    }
}
