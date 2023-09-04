package com.taobao.search.iquan.core.api.schema;

import com.taobao.search.iquan.client.common.common.ConstantDefine;
import com.taobao.search.iquan.core.api.exception.TableNotExistException;
import com.taobao.search.iquan.core.common.Range;
import com.taobao.search.iquan.client.common.json.table.JsonLayerTable;
import com.taobao.search.iquan.client.common.model.IquanLayerTableModel;
import com.taobao.search.iquan.core.api.SqlTranslator;
import com.taobao.search.iquan.core.api.exception.ExceptionUtils;
import com.taobao.search.iquan.core.api.exception.IquanNotValidateException;
import com.taobao.search.iquan.core.catalog.GlobalCatalog;
import com.taobao.search.iquan.core.catalog.GlobalCatalogManager;
import com.taobao.search.iquan.core.catalog.ObjectPath;
import org.apache.calcite.sql.SqlKind;
import java.util.*;

public class LayerTable {
    private String layerTableName;
    private List<Layer> layers;
    private List<LayerFormat> layerFormats;
    private Map<String, Object> properties;
    private Table fakeTable;
    private IquanLayerTableModel layerTableModel;
    private JsonLayerTable jsonLayerTable;
    private List<Object> layerTableMeta;
    private Integer schemaId;

    public List<Layer> getLayers() {
        return layers;
    }

    public LayerTable setLayers(List<Layer> layers) {
        this.layers = layers;
        return this;
    }

    public List<LayerFormat> getLayerFormats() {
        return layerFormats;
    }

    public LayerTable setLayerFormats(List<LayerFormat> layerFormats) {
        this.layerFormats = layerFormats;
        return this;
    }

    public String getLayerTableName() {
        return layerTableName;
    }

    public LayerTable setLayerTableName(String layerTableName) {
        this.layerTableName = layerTableName;
        return this;
    }

    public Table getFakeTable() {
        return fakeTable;
    }

    public LayerTable setFakeTable(Table fakeTable) {
        this.fakeTable = fakeTable;
        return this;
    }

    public JsonLayerTable getJsonLayerTable() {
        return jsonLayerTable;
    }

    public LayerTable setJsonLayerTable(JsonLayerTable jsonLayerTable) {
        this.jsonLayerTable = jsonLayerTable;
        return this;
    }

    public Integer getSchemaId() {
        if (schemaId != null) {
            return schemaId;
        }
        if (properties != null) {
            Object id = properties.get("schema_standard");
            if (id != null) {
                schemaId = (Integer) id;
                return schemaId;
            }
        }
        schemaId = 0;
        return schemaId;
    }

    public boolean validateLayers(SqlTranslator sqlTranslator, List<AbstractField> fields) throws TableNotExistException {
        GlobalCatalogManager manager = sqlTranslator.getCatalogManager();
        List<Table> tables = new ArrayList<>();
        for (Map.Entry<String, GlobalCatalog> entry : manager.getCatalogMap().entrySet()) {
            GlobalCatalog catalog = entry.getValue();
            for (Layer layer : layers) {
                ObjectPath path = new ObjectPath(layer.getDbName(), layer.getTableName());
                if (!catalog.tableExists(layer.getDbName(), layer.getTableName())) {
                    return false;
                }
                tables.add(catalog.getTable(layer.getDbName(), layer.getTableName()).getTable());
            }
            Table iquanTable = tables.get(getSchemaId());
            fields.clear();
            fields.addAll(iquanTable.getFields());

            for (LayerFormat format : layerFormats) {
                FieldType fieldType = format.getLayerInfoValueType() == LayerInfoValueType.INT_DISCRETE ||
                        format.getLayerInfoValueType() == LayerInfoValueType.INT_RANGE ?
                        FieldType.FT_INT64 : FieldType.FT_STRING;
                fields.add(new AtomicField(format.getFieldName(), fieldType, false));
            }
            return true;
        }
        return false;
    }

    private void genNormalizedLayerInfo() {
        layerTableMeta = new ArrayList<>(layerFormats.size());
        for (int idx = 0; idx < layerFormats.size(); ++idx) {
            LayerFormat layerFormat = layerFormats.get(idx);
            if (layerFormat.getValueType().equals(ConstantDefine.STRING)) {
                List<String> strs = new ArrayList<>();
                layerTableMeta.add(strs);
                for (Layer layer : layers) {
                    strs.addAll(layer.getLayerInfos().get(idx).getStringDiscreteValue());
                }
            } else if (layerFormats.get(idx).getValueType().equals(ConstantDefine.INT)) {
                List<Long> integers = new ArrayList<>();
                integers.add(Long.MIN_VALUE);
                integers.add(Long.MAX_VALUE);
                if (layerFormat.getLayerInfoValueType() == LayerInfoValueType.INT_DISCRETE) {
                    for (Layer layer : layers) {
                        integers.addAll(layer.getLayerInfos().get(idx).getIntDiscreteValue());
                    }
                } else {
                    for (Layer layer : layers) {
                        for (Range range : layer.getLayerInfos().get(idx).getIntRangeValue()) {
                            integers.add(range.getLeft());
                            integers.add(range.getRight());
                        }
                    }
                }
                Collections.sort(integers);
                layerTableMeta.add(integers);
            }
        }
    }

    private List<Object> getLayerTableMeta() {
        if (layerTableMeta == null) {
            genNormalizedLayerInfo();
        }
        return layerTableMeta;
    }

    public Object getNormalizedValue(int fieldId, SqlKind op, Object value, boolean reverse) {
        ExceptionUtils.throwIfTrue(fieldId >= layerFormats.size(), "layerTable : index out of field range");
        LayerFormat layerFormat = layerFormats.get(fieldId);
        if (layerFormat.getValueType().equals(ConstantDefine.STRING)) {
            String strValue = (String) value;
            return ((List<String>) (layerTableMeta.get(fieldId))).contains(strValue) ? strValue : "****NOT$EXIST****";
        }
        if (layerFormat.getValueType().equals(ConstantDefine.INT)) {
            Long integerValue = ((Number) value).longValue();
            switch (op) {
                case EQUALS: {
                    Long lessOrEqual = getLessValue(fieldId, integerValue);
                    return Objects.equals(lessOrEqual, integerValue) ? integerValue : lessOrEqual + 1;
                }
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL: {
                    boolean greater = (op == SqlKind.GREATER_THAN || op == SqlKind.GREATER_THAN_OR_EQUAL);
                    return greater == reverse ? getLessValue(fieldId, integerValue) : getGreaterValue(fieldId, integerValue);
                }
            }
        }
        throw new IquanNotValidateException("layerTable field only support ops := > >= < <=");
    }

    private Long getGreaterValue(int fieldId, Long value) {
        List<Long> integers = (List<Long>) getLayerTableMeta().get(fieldId);
        return firstGreaterEqual(integers, value);
    }

    private Long getLessValue(int fieldId, Long value) {
        List<Long> integers = (List<Long>) getLayerTableMeta().get(fieldId);
        return firstLessEqual(integers, value);
    }

    static public Long firstGreaterEqual(List<Long> integers, Long value) {
        int l = 0;
        int r = integers.size() - 1;
        while(l < r) {
            int mid = (l + r) >> 1;
            Long midValue = integers.get(mid);
            if (midValue < value) {
                l = mid + 1;
            } else {
                r = mid;
            }
        }
        return integers.get(l);
    }

    static public Long firstLessEqual(List<Long> integers, Long value) {
        int l = 0;
        int r = integers.size() - 1;
        while (l < r) {
            int mid = (l + r + 1) >> 1;
            Long midValue = integers.get(mid);
            if (midValue > value) {
                r = mid - 1;
            } else {
                l = mid;
            }
        }
        return integers.get(l);
    }
    public Map<String, Object> getProperties() {
        return properties;
    }

    public LayerTable setProperties(Map<String, Object> properties) {
        this.properties = properties;
        return this;
    }

    public IquanLayerTableModel getLayerTableModel() {
        return layerTableModel;
    }

    public void setLayerTableModel(IquanLayerTableModel layerTableModel) {
        this.layerTableModel = layerTableModel;
    }
}
