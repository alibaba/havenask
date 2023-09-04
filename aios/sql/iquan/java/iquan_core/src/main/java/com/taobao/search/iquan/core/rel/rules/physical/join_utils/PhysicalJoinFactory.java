package com.taobao.search.iquan.core.rel.rules.physical.join_utils;

import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.api.schema.Table;
import com.taobao.search.iquan.core.rel.ops.physical.IquanJoinOp;
import org.apache.calcite.rel.RelNode;

public abstract class PhysicalJoinFactory {
    final protected PhysicalJoinType physicalJoinType;

    public PhysicalJoinFactory(PhysicalJoinType physicalJoinType) {
        this.physicalJoinType = physicalJoinType;
    }

    public abstract RelNode create(IquanJoinOp joinOp, Table leftTable, Table rightTable);

    public static PhysicalJoinFactory createInstance(JsonResult result) {
        switch (result.physicalJoinType) {
            case HASH_JOIN:
                return new HashJoinFactory(
                        result.physicalJoinType,
                        result.leftIsBuild,
                        result.tryDistinctBuildRow
                );
            case LOOKUP_JOIN:
                return new LookupJoinFactory(
                        result.physicalJoinType,
                        result.leftIsBuild,
                        result.isInternalBuild
                );
            default:
                throw new SqlQueryException(IquanErrorCode.IQUAN_FAIL, result.physicalJoinType + " is not support");
        }
    }
}
