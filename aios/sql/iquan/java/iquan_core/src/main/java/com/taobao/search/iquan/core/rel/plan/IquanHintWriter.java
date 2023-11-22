package com.taobao.search.iquan.core.rel.plan;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.hint.IquanHintCategory;
import com.taobao.search.iquan.core.rel.hint.IquanHintOptUtils;
import com.taobao.search.iquan.core.rel.ops.physical.ExecLookupJoinOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanAggregateOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanHashJoinOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanJoinOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanNestedLoopJoinOp;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableScanBase;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;

public class IquanHintWriter {
    private final Map<RelNode, Integer> relIdMap = new IdentityHashMap<>();
    private List<String> outputs;
    private boolean detail;

    public IquanHintWriter() {
        outputs = new ArrayList<>();
        outputs.add(ConstantDefine.EMPTY);
        detail = false;
    }

    public IquanHintWriter(boolean detail) {
        outputs = new ArrayList<>();
        outputs.add(ConstantDefine.EMPTY);
        this.detail = detail;
    }

    public String asString(List<RelNode> roots) {
        for (RelNode root : roots) {
            visit(root, 0);
        }
        outputs.add(ConstantDefine.EMPTY);
        return String.join(ConstantDefine.NEW_LINE, outputs);
    }

    public Object asObject(List<RelNode> roots) {
        return asString(roots);
    }

    private String dumpHintByCategories(RelNode rel, List<IquanHintCategory> categories) {
        if (!(rel instanceof Hintable) || categories.isEmpty()) {
            return "[]";
        }

        List<RelHint> hints = new ArrayList<>();
        for (IquanHintCategory category : categories) {
            RelHint hint = IquanHintOptUtils.resolveHints(rel, category);
            if (hint != null) {
                hints.add(hint);
            }
        }

        return hints.toString();
    }

    private void visit(RelNode node, int spaceNum) {
        int index = outputs.size();
        outputs.add("");

        if (node instanceof HepRelVertex) {
            RelNode currentNode = ((HepRelVertex) node).getCurrentRel();
            visit(currentNode, spaceNum + 4);
        } else {
            for (RelNode input : node.getInputs()) {
                visit(input, spaceNum + 4);
            }
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < spaceNum; ++i) {
            sb.append(ConstantDefine.SPACE);
        }
        sb.append(node.getRelTypeName());
        Integer id = relIdMap.get(node);
        if (id == null) {
            id = relIdMap.size();
            relIdMap.put(node, id);
        }
        sb.append(ConstantDefine.TABLE_SEPARATOR);
        sb.append(id);

        sb.append("(");
        if (node instanceof TableScan) {
            TableScan scan = (TableScan) node;
            sb.append(scan.getTable().getQualifiedName().toString());
            sb.append(",");
            sb.append(dumpHintByCategories(node, ImmutableList.of(IquanHintCategory.CAT_INDEX, IquanHintCategory.CAT_LOCAL_PARALLEL, IquanHintCategory.CAT_JOIN, IquanHintCategory.CAT_SCAN_ATTR)));
        } else if (node instanceof ExecLookupJoinOp) {
            ExecLookupJoinOp lookupJoinOp = (ExecLookupJoinOp) node;
            IquanTableScanBase buildScan = lookupJoinOp.getBuildOp();
            sb.append("buildNode={");
            sb.append(buildScan.getTable().getQualifiedName().toString());
            sb.append(",");
            sb.append(dumpHintByCategories(buildScan, ImmutableList.of(IquanHintCategory.CAT_INDEX, IquanHintCategory.CAT_LOCAL_PARALLEL, IquanHintCategory.CAT_JOIN, IquanHintCategory.CAT_SCAN_ATTR)));
            sb.append("},");
            IquanJoinOp joinOp = lookupJoinOp.getJoinOp();
            sb.append(dumpHintByCategories(joinOp, ImmutableList.of(IquanHintCategory.CAT_JOIN_ATTR)));
        } else if (node instanceof IquanHashJoinOp) {
            IquanHashJoinOp hashJoin = (IquanHashJoinOp) node;
            sb.append("leftIsBuild=");
            sb.append(hashJoin.isLeftIsBuild());
            sb.append(",");
            sb.append(dumpHintByCategories(node, ImmutableList.of(IquanHintCategory.CAT_JOIN_ATTR)));
        } else if (node instanceof IquanNestedLoopJoinOp) {
            IquanNestedLoopJoinOp nestedLoopJoin = (IquanNestedLoopJoinOp) node;
            sb.append("leftIsBuild=");
            sb.append(nestedLoopJoin.isLeftIsBuild());
            sb.append(",");
            sb.append(dumpHintByCategories(node, ImmutableList.of(IquanHintCategory.CAT_JOIN_ATTR)));
        } else if (node instanceof IquanAggregateOp) {
            sb.append(dumpHintByCategories(node, ImmutableList.of(IquanHintCategory.CAT_AGG, IquanHintCategory.CAT_AGG_ATTR)));
        }
        sb.append(")");
        outputs.set(index, sb.toString());
    }
}
