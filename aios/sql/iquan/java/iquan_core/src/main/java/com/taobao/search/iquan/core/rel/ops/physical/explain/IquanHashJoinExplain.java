package com.taobao.search.iquan.core.rel.ops.physical.explain;

import java.util.Map;

import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.ops.physical.IquanHashJoinOp;
import org.apache.calcite.sql.SqlExplainLevel;

public class IquanHashJoinExplain extends IquanJoinExplain {

    public IquanHashJoinExplain(IquanHashJoinOp join) {
        super(join);
    }

    @Override
    protected void explainJoinInternalAttrs(Map<String, Object> map, SqlExplainLevel level) {
        super.explainJoinInternalAttrs(map, level);

        IquanHashJoinOp hashJoinOp = (IquanHashJoinOp) join;
        map.put(ConstantDefine.LEFT_IS_BUILD, hashJoinOp.isLeftIsBuild());
        //map.put(ConstantDefine.IS_BROADCAST, hashJoinOp.isBroadcast());
        if (level == SqlExplainLevel.ALL_ATTRIBUTES) {
            map.put(ConstantDefine.TRY_DISTINCT_BUILD_ROW, hashJoinOp.isTryDistinctBuildRow());
        }
    }
}
