package com.taobao.search.iquan.core.utils;

import java.util.List;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.NlsString;

public class IquanMiscUtils {
    public static Integer getTopKFromSortTvf(RexCall sortTvf) {
        List<RexNode> operands = sortTvf.getOperands();
        if (operands.size() < 2) {
            return null;
        }
        RexNode topKRex = operands.get(1);
        if (!(topKRex instanceof RexLiteral)) {
            return null;
        }
        RexLiteral topKLiteral = (RexLiteral) topKRex;
        if (!(topKLiteral.getValue4() instanceof NlsString)) {
            return null;
        }
        String topKStr = ((NlsString) topKLiteral.getValue4()).getValue();
        try {
            return Integer.parseInt(topKStr);
        } catch (NumberFormatException ex) {
            return null;
        }
    }
}
