package com.taobao.search.iquan.core.rel.ops.logical.LayerTable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.FunctionNotExistException;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.api.schema.TableType;
import com.taobao.search.iquan.core.catalog.GlobalCatalog;
import com.taobao.search.iquan.core.rel.programs.IquanOptContext;
import com.taobao.search.iquan.core.utils.LayerTableUtils;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.StringUtils;

public class LayerTableAggDistinct extends LayerTableDistinct {
    protected final String primaryFuncName;
    protected final String defaultFuncName;

    protected final String hintName;

    protected LayerTableAggDistinct(LogicalLayerTableScan scan) {
        super(scan);
        Map<String, String> distinctParam = (Map<String, String>) distinct.get("params");
        this.primaryFuncName = distinctParam.get("primary");
        this.defaultFuncName = distinctParam.get("default");
        this.hintName = distinctParam.get("hint");
        if (this.defaultFuncName == null) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_LAYER_TABLE_PROPERTIES_BIZ
                    , "layerTable [%s]: properties->distinct have no default func");
        }
    }

    @Override
    public List<String> addedFiledNames() {
        return Collections.singletonList(LayerTableUtils.getPkFieldName(scan));
    }

    @Override
    public RelNode addDistinctNode(RelNode input, RelOptRuleCall call, RelDataType outputRowType, int offset) throws FunctionNotExistException {
        int groupId = LayerTableUtils.getGroupId(scan, input.getRowType()) + offset;
        String hintName = com.taobao.search.iquan.core.common.ConstantDefine.NORMAL_AGG_HINT;
        boolean normalAgg = hintName.equals(this.hintName);
        Context originalContext = call.getPlanner().getContext();
        IquanOptContext context = originalContext.unwrap(IquanOptContext.class);
        GlobalCatalog catalog = context.getCatalogManager().getCatalog(context.getExecutor().getDefaultCatalogName());

        SqlAggFunction arbitraryFunc = (SqlAggFunction) catalog.getFunction(context.getExecutor().getDefaultDbName(), defaultFuncName).build();
        SqlAggFunction function = arbitraryFunc;
        if (StringUtils.equals("max", primaryFuncName)) {
            function = SqlStdOperatorTable.MAX;
        } else if (StringUtils.equals("min", primaryFuncName)) {
            function = SqlStdOperatorTable.MIN;
        }

        List<AggregateCall> calls = new ArrayList<>();
        for (int i = 0; i < input.getRowType().getFieldCount(); ++i) {
            if (i != groupId) {
                RelDataType originalType = input.getRowType().getFieldList().get(i).getType();
                boolean isNumeric = originalType.getSqlTypeName().getFamily().equals(SqlTypeFamily.NUMERIC);
                String filedName = input.getRowType().getFieldList().get(i).getName();
                SqlAggFunction curFunction = isNumeric ? function : arbitraryFunc;
                AggregateCall aggCall = AggregateCall.create(curFunction,
                        false, false, false,
                        ImmutableList.of(i), -1, RelCollations.EMPTY, originalType, filedName);
                calls.add(aggCall);
            }
        }
        RelBuilder relBuilder = call.builder();
        relBuilder.clear();
        final RelBuilder.GroupKey groupKey =
                relBuilder.push(input).groupKey(groupId);
        RelNode agg = relBuilder.aggregate(groupKey, calls).build();
        if (normalAgg && (agg instanceof LogicalAggregate)) {
            RelHint relHint = RelHint.builder(hintName).hintOption(TableType.Constant.LAYER_TABLE).build();
            agg = ((LogicalAggregate) agg).withHints(Collections.singletonList(relHint));
        }
        return LayerTableUtils.genProjectWithName(relBuilder, agg, outputRowType);
    }
}
