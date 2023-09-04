package com.taobao.search.iquan.core.rel.visitor.rexshuttle;

import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.api.schema.LayerTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RexLayerDynamicParamsShuttle extends RexShuttle {
    private static final Logger logger = LoggerFactory.getLogger(RexLayerDynamicParamsShuttle.class);

    private final RexBuilder rexBuilder;
    private final List<List<Object>> dynamicParams;
    private final int limit;
    private LayerTable layerTable;

    private List<Map<String, Object>> layerTablePlanMeta;

    public RexLayerDynamicParamsShuttle(RexBuilder rexBuilder,
                                        List<List<Object>> dynamicParams,
                                        int limit,
                                        LayerTable layerTable,
                                        List<Map<String, Object>> layerTablePlanMeta) {
        this.rexBuilder = rexBuilder;
        this.dynamicParams = dynamicParams;
        this.limit = limit;
        this.layerTable = layerTable;
        this.layerTablePlanMeta = layerTablePlanMeta;
    }

    public RexBuilder getRexBuilder() {
        return rexBuilder;
    }

    @Override
    public RexNode visitCall(RexCall call) {
        SqlKind op = call.getKind();
        switch (op) {
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case EQUALS:{
                RexNode first = call.getOperands().get(0);
                int flag = first instanceof RexInputRef ? 0 : 1;
                RexInputRef inputRef = RexShuttleUtils.getInputRefFromCall(call);
                RexDynamicParam right = RexShuttleUtils.getDynamicParamFromCall(call);

                if (inputRef == null || inputRef.getIndex() < limit || right == null) {
                    return super.visitCall(call);
                }

                int fieldId = inputRef.getIndex() - limit;
                Map<String, Object> slot = new HashMap<>();
                layerTablePlanMeta.add(slot);
                slot.put("layer_table_name", layerTable.getLayerTableName());
                slot.put("dynamic_params_index", right.getIndex());
                slot.put("layer_field", layerTable.getLayerFormats().get(fieldId).getFieldName());
                slot.put("op", op.toString());

                List<RexNode> newOperands = new ArrayList<>();
                if (flag == 0) {
                    slot.put("reverse", false);
                    newOperands.add(inputRef);
                    newOperands.add(processRexDynamicParam(right, fieldId, op, false, slot));
                } else {
                    slot.put("reverse", true);
                    newOperands.add(processRexDynamicParam(right, fieldId, op, true, slot));
                    newOperands.add(inputRef);
                }
                return rexBuilder.makeCall(call.getType(), call.getOperator(), newOperands);
            }
        }
        return super.visitCall(call);
    }

    private RexNode processRexDynamicParam(RexDynamicParam dynamicParam, int fieldId , SqlKind op, boolean reverse, Map<String, Object> slot) {
        List<Object> params = dynamicParams.get(0);

        int i = dynamicParam.getIndex();
        if (i >= params.size()) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_DYNAMIC_PARAMS_LACK_OF_INPUT,
                    String.format("sql[%d], param[%d], param_size[%d]", 0, i, params.size()));
        }

        RelDataType relDataType = dynamicParam.getType();
        SqlTypeName sqlType = relDataType.getSqlTypeName();
        SqlTypeFamily sqlTypeFamily = sqlType.getFamily();
        Object actualParam = params.get(i);

        try {
            if (sqlTypeFamily == SqlTypeFamily.NUMERIC) {
                BigDecimal value = new BigDecimal(String.valueOf(actualParam));
                slot.put("normalized_value", layerTable.getNormalizedValue(fieldId, op, value.longValue(), reverse));
                return rexBuilder.makeExactLiteral(value, relDataType);
            } else if (sqlTypeFamily == SqlTypeFamily.CHARACTER) {
                String value = (String) actualParam;
                slot.put("normalized_value", layerTable.getNormalizedValue(fieldId, op, value, reverse));
                return rexBuilder.makeLiteral(value);
            }
        } catch (Exception ex) {
            logger.error("cast dynamic params fail:", ex);
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_DYNAMIC_PARAMS_INVALID_PARAMS,
                    String.format("sql[0], param[%d], value[%s], type[%s], family[%s]",
                            i, actualParam.toString(), sqlType.toString(), sqlTypeFamily.toString())
            );
        }

        throw new SqlQueryException(IquanErrorCode.IQUAN_EC_DYNAMIC_PARAMS_UNSUPPORT_PARAM_TYPE,
                String.format("sql[0], param[%d], value[%s], type[%s], family[%s]",
                        i, actualParam.toString(), sqlType.toString(), sqlTypeFamily.toString())
        );
    }

    public void setLayerTable(LayerTable layerTable) {
        this.layerTable = layerTable;
    }
}
