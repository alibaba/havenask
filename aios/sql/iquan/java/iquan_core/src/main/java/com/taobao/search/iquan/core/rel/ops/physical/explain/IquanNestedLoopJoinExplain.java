package com.taobao.search.iquan.core.rel.ops.physical.explain;

import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.ops.physical.IquanNestedLoopJoinOp;
import org.apache.calcite.sql.SqlExplainLevel;

import java.util.Map;

public class IquanNestedLoopJoinExplain extends IquanJoinExplain {

    public IquanNestedLoopJoinExplain(IquanNestedLoopJoinOp join) {
        super(join);
    }

    @Override
    protected void explainJoinInternalAttrs(Map<String, Object> map, SqlExplainLevel level) {
        super.explainJoinInternalAttrs(map, level);

        IquanNestedLoopJoinOp nestedLoopJoinOp = (IquanNestedLoopJoinOp) join;
        map.put(ConstantDefine.LEFT_IS_BUILD, nestedLoopJoinOp.isLeftIsBuild());
        map.put(ConstantDefine.IS_INTERNAL_BUILD, nestedLoopJoinOp.isInternalBuild());
    }
}
