package com.taobao.search.iquan.core.rel.plan;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;

public class IquanThumbWriter {
    private final Map<RelNode, Integer> relIdMap = new IdentityHashMap<>();
    private List<String> outputs;
    private boolean detail;

    public IquanThumbWriter() {
        outputs = new ArrayList<>();
        outputs.add(ConstantDefine.EMPTY);
        detail = false;
    }

    public IquanThumbWriter(boolean detail) {
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
        if (detail) {
            sb.append(IquanRelOptUtils.getRelNodeName(node))
                    .append(ConstantDefine.COLON)
                    .append(node.getClass().getSimpleName())
                    .append(ConstantDefine.COLON)
                    .append(node.getId());
        } else {
            sb.append(IquanRelOptUtils.getRelNodeName(node));
        }
        Integer id = relIdMap.get(node);
        if (id == null) {
            id = relIdMap.size();
            relIdMap.put(node, id);
        }
        sb.append(ConstantDefine.TABLE_SEPARATOR);
        sb.append(id);
        outputs.set(index, sb.toString());
    }
}
