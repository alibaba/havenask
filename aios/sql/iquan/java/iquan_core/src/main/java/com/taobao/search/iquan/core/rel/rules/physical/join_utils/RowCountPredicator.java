package com.taobao.search.iquan.core.rel.rules.physical.join_utils;

import com.taobao.search.iquan.core.api.schema.IquanTable;
import com.taobao.search.iquan.core.rel.ops.physical.IquanTableScanBase;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;

public class RowCountPredicator {
    final static private long TINY = 100;
    final static private long SMALL = 200;
    final static private long MIDDLE = 300;
    final static private long HUGE = 400;

    /**
     * 1. scan with 1 partition
     * 2. non-scan
     * 3. scan with multi-partition and condition
     * 4. scan with multi-partition and non-condition
     */
    public long rowCount(RelNode op) {
        if (!(op instanceof TableScan)) {
            return SMALL;
        }

        IquanTable iquanTable = IquanRelOptUtils.getIquanTable(op);
        assert iquanTable != null;
        int partitionCnt = iquanTable.getDistribution().getPartitionCnt();
        if (partitionCnt == 1) {
            return TINY;
        }

        RexNode condition = null;
        if (op instanceof IquanTableScanBase) {
            RexProgram rexProgram = ((IquanTableScanBase) op).getNearestRexProgram();
            if (rexProgram != null) {
                condition = rexProgram.getCondition();
            }
        }
        return condition == null ? HUGE * partitionCnt : MIDDLE * partitionCnt;
    }
}
