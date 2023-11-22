package com.taobao.search.iquan.core.rel.ops.logical.LayerTable;

import java.util.List;
import java.util.Map;

import com.taobao.search.iquan.core.api.exception.FunctionNotExistException;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

public abstract class LayerTableDistinct {
    protected final LogicalLayerTableScan scan;
    protected final Map<String, Object> distinct;

    protected LayerTableDistinct(LogicalLayerTableScan scan) {
        this.scan = scan;
        Map<String, Object> properties = scan.getLayerTable().getProperties();
        this.distinct = (Map<String, Object>) properties.get("distinct");
    }

    public abstract List<String> addedFiledNames();

    public abstract RelNode addDistinctNode(RelNode inputNode, RelOptRuleCall call, RelDataType outputRowType, int offset) throws FunctionNotExistException;
}
