package com.taobao.search.iquan.core.api.schema;

import com.taobao.search.iquan.client.common.json.common.JsonAggregationIndex;
import com.taobao.search.iquan.client.common.json.common.JsonIndex;
import com.taobao.search.iquan.client.common.json.common.JsonIndexes;
import com.taobao.search.iquan.client.common.json.table.JsonTable;
import com.taobao.search.iquan.client.common.model.IquanTableModel;
import com.taobao.search.iquan.core.rel.plan.PlanWriteUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Table {
    private static final Logger logger = LoggerFactory.getLogger(Table.class);

    private long version = -1;
    private String tableName = null;
    private String originalName = null;
    private TableType tableType = TableType.TT_INVALID;
    private List<AbstractField> fields = null;
    private Distribution distribution = null;
    private Location location = null;
    private List<SortDesc> sortDescs = null;
    private List<JoinInfo> joinInfo= null;
    private Map<String, String> properties = new HashMap<>();
    private JsonTable jsonTable = null;
    private IquanTableModel tableModel = null;
    // calc internal
    private final List<FieldMeta> fieldMetaList = new ArrayList<>();
    private Map<String, IndexType> fieldToIndexMap = null;
    private Map<String, FieldMeta> primaryMap = new HashMap<>();
    private JsonIndexes indexes = null;
    private int subTablesCnt = 0;

    private Table() {
    }

    public long getVersion() {
        return version;
    }

    public String getTableName() {
        return tableName;
    }

    public String getOriginalName() {
        return originalName;
    }

    public TableType getTableType() {
        return tableType;
    }

    public List<AbstractField> getFields() {
        return fields;
    }

    public Distribution getDistribution() {
        return distribution;
    }

    public Location getLocation() {
        return location;
    }

    public List<JoinInfo> getJoinInfos() {
        return joinInfo;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public JsonTable getJsonTable() {
        return jsonTable;
    }

    public IquanTableModel getTableModel() {
        return tableModel;
    }

    public List<FieldMeta> getFieldMetaList() {
        return fieldMetaList;
    }

    public Map<String, FieldMeta> getPrimaryMap() {
        return primaryMap;
    }

    public Map<String, IndexType> getFieldToIndexMap() {
        return fieldToIndexMap;
    }

    private void setVersion(long version) {
        this.version = version;
    }

    private void setTableName(String tableName) {
        this.tableName = tableName;
    }

    private void setOriginalName(String originalName) {
        this.originalName = originalName;
    }

    public void setTableType(String tableType) {
        this.tableType = TableType.from(tableType);
    }

    private void setFields(List<AbstractField> fields) {
        this.fields = fields;
    }

    private void addField(AbstractField field) {
        if (this.fields == null) {
            this.fields = new ArrayList<>();
        }
        this.fields.add(field);
    }

    private void setDistribution(Distribution distribution) {
        this.distribution = distribution;
    }

    private void setLocation(Location location) {
        this.location = location;
    }

    public void setJoinInfos(List<JoinInfo> joinInfos) {
        this.joinInfo = joinInfos;
    }

    private void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    private void addProperties(Map<String, String> properties) {
        if (this.properties == null) {
            this.properties = new HashMap<>();
        }
        this.properties.putAll(properties);
    }

    public void setJsonTable(JsonTable jsonTable) {
        this.jsonTable = jsonTable;
    }

    public void setTableModel(IquanTableModel tableModel) {
        this.tableModel = tableModel;
    }

    public void setIndexes(JsonIndexes indexes) { this.indexes = indexes;   }

    public JsonIndexes getIndexes() {return this.indexes;   }

    public String getDigest() {
        String joinInfosStr = "[";
        if (joinInfo != null) {
            for (JoinInfo joinInfoItem : joinInfo) {
                joinInfosStr += "{" + joinInfoItem.getDigest() + "} ,";
            }
        }
        joinInfosStr += "]";
        return String.format("tableName=%s, tableType=%s, version=%d, fields=[%s], distribution=[%s], location=[%s], joinInfos=[%s]",
                tableName, tableType.getName(), version, fields.toString(), distribution.getDigest(), location.getDigest(), joinInfosStr);
    }

    @Override
    public String toString() {
        return getDigest();
    }

    public boolean isValid() {
        if (version < 0 || tableName == null || tableName.isEmpty() || !tableType.isValid() || fields == null || fields.isEmpty()) {
            logger.error("table is not valid: {}", this.getDigest());
            return false;
        }

        if (!tableType.equals(TableType.TT_LAYER_TABLE)) {
            if (distribution == null || !distribution.isValid()) {
                logger.error("distribution is not valid: {}", this.getDigest());
                return false;
            }

            if (location == null || !location.isValid()) {
                logger.error("location is not valid: {}", this.getDigest());
                return false;
            }
        }

        for (AbstractField field : fields) {
            if (!field.isValid()) {
                logger.error("field is not valid: field {}, table {}", field.getDigest(), this.getDigest());
                return false;
            }
        }
        return true;
    }

    private void postProcess() {
        for (AbstractField field : fields) {
            fillFieldMeta(field,0);
        }
        fieldToIndexMap = new HashMap<>(fieldMetaList.size()*2);
        for (FieldMeta fieldMeta : fieldMetaList) {
            fieldToIndexMap.put(fieldMeta.originFieldName, fieldMeta.indexType);
            switch (fieldMeta.indexType) {
                case IT_PRIMARY_KEY:
                case IT_PRIMARYKEY64:
                case IT_PRIMARYKEY128:
                    primaryMap.put(fieldMeta.fieldName, fieldMeta);
                    break;
                default:
                    break;
            }
        }
    }

    private void fillFieldMeta(AbstractField field, int curLevel) {
        if (curLevel > 1) {
            return;
        }

        if (field.isAtomicType()) {
            AtomicField atomicField = (AtomicField) field;
            FieldMeta fieldInfo = new FieldMeta();
            fieldInfo.fieldName = PlanWriteUtils.formatFieldName(atomicField.getFieldName());
            fieldInfo.originFieldName = atomicField.getFieldName();
            fieldInfo.fieldType = atomicField.getFieldType();
            fieldInfo.indexName = atomicField.getIndexName();
            fieldInfo.indexType = atomicField.getIndexType();
            fieldInfo.isAttribute = atomicField.getIsAttribute();
            fieldMetaList.add(fieldInfo);
            return;
        } else if (field.isRowType()) {
            List<AbstractField> recordFields = ((RowField) field).getFields();
            for (AbstractField recordField : recordFields) {
                fillFieldMeta(recordField, curLevel + 1);
            }
        } else if (field.isArrayType()) {
            FieldMeta fieldInfo = new FieldMeta();
            fieldInfo.fieldName = PlanWriteUtils.formatFieldName(field.getFieldName());
            fieldInfo.originFieldName = field.getFieldName();
            fieldInfo.fieldType = field.getFieldType();
            fieldInfo.isAttribute = field.getIsAttribute();
            AbstractField elementField = ((ArrayField) field).getElementField();
            if (elementField.isAtomicType()) {
                AtomicField atomicField = (AtomicField) elementField;
                fieldInfo.indexName = atomicField.getIndexName();
                fieldInfo.indexType = atomicField.getIndexType();
                fieldInfo.isAttribute = atomicField.getIsAttribute();
                fieldMetaList.add(fieldInfo);
            }
        } else if (field.isMultiSetType()) {
            FieldMeta fieldInfo = new FieldMeta();
            fieldInfo.fieldName = PlanWriteUtils.formatFieldName(field.getFieldName());
            fieldInfo.originFieldName = field.getFieldName();
            fieldInfo.fieldType = field.getFieldType();
            fieldInfo.isAttribute = false;
            fieldMetaList.add(fieldInfo);
            fillFieldMeta(((MultiSetField) field).getElementField(), curLevel);
        } else if (field.isMapType()) {
            FieldMeta fieldInfo = new FieldMeta();
            fieldInfo.fieldName = PlanWriteUtils.formatFieldName(field.getFieldName());
            fieldInfo.originFieldName = field.getFieldName();
            fieldInfo.fieldType = field.getFieldType();
            fieldInfo.isAttribute = field.getIsAttribute();
            fieldMetaList.add(fieldInfo);
            return;
        }
    }

    public String getPkIndexName() {
        for (FieldMeta fieldMeta : fieldMetaList) {
            if (fieldMeta.indexType.isPrimaryKey()) {
                return fieldMeta.indexName;
            }
        }
        return null;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(Table table) {
        return new Builder(table);
    }

    public List<SortDesc> getSortDescs() {
        return sortDescs;
    }

    public void setSortDescs(List<SortDesc> sortDescs) {
        this.sortDescs = sortDescs;
    }

    public int getSubTablesCnt() {
        return subTablesCnt;
    }

    public void setSubTablesCnt(int subTablesCnt) {
        this.subTablesCnt = subTablesCnt;
    }

    public static class Builder {
        private Table table;
        private boolean deepCopy = false;

        public Builder() {
            table = new Table();
        }

        public Builder(Table table) {
            this();

            this.table.version = table.getVersion();
            this.table.tableName = table.getTableName();
            this.table.originalName = table.getOriginalName();
            this.table.tableType = table.getTableType();
            this.table.fields = table.getFields();
            this.table.distribution = table.getDistribution();
            this.table.sortDescs = table.getSortDescs();
            this.table.location = table.getLocation();
            this.table.joinInfo = table.getJoinInfos();
            this.table.properties = table.getProperties();
            this.table.jsonTable = table.getJsonTable();
            this.table.tableModel = table.getTableModel();
            this.deepCopy = true;
            this.table.subTablesCnt = table.getSubTablesCnt();
        }

        public Builder version(long version) {
            table.setVersion(version);
            return this;
        }

        public Builder tableName(String tableName) {
            table.setTableName(tableName);
            return this;
        }

        public Builder originalName(String originalName) {
            table.setOriginalName(originalName);
            return this;
        }

        public Builder subTablesCnt(int cnt) {
            table.setSubTablesCnt(cnt);
            return this;
        }

        public Builder tableType(String tableType) {
            if (!deepCopy) {
                table.setTableType(tableType);
            }
            return this;
        }

        public Builder fields(List<AbstractField> fields) {
            if (!deepCopy) {
                table.setFields(fields);
            }
            return this;
        }

        public Builder addField(AbstractField field) {
            if (!deepCopy) {
                table.addField(field);
            }
            return this;
        }

        public Builder distribution(Distribution distribution) {
            if (!deepCopy) {
                table.setDistribution(distribution);
            }
            return this;
        }

        public Builder location(Location location) {
            if (!deepCopy) {
                table.setLocation(location);
            }
            return this;
        }

        public Builder sortDescs(List<SortDesc> sortDescs) {
            if (!deepCopy) {
                table.setSortDescs(sortDescs);
            }
            return this;
        }

        public Builder joinInfos(List<JoinInfo> joinInfos) {
            if (!deepCopy) {
                table.setJoinInfos(joinInfos);
            }
            return this;
        }

        public Builder properties(Map<String, String> properties) {
            if (!deepCopy) {
                table.setProperties(properties);
            }
            return this;
        }

        public Builder addProperties(Map<String, String> properties) {
            if (!deepCopy) {
                table.addProperties(properties);
            }
            return this;
        }

        public Builder jsonTable(JsonTable jsonTable) {
            if (!deepCopy) {
                table.setJsonTable(jsonTable);
            }
            return this;
        }

        public Builder tableModel(IquanTableModel tableModel) {
            if (!deepCopy) {
                table.setTableModel(tableModel);
            }
            return this;
        }

        public Builder indexes(JsonIndexes indexes) {
            if (!deepCopy) {
                table.setIndexes(indexes);
            }
            return this;
        }

        public Table build() {
            if (table.isValid()) {
                table.postProcess();
                return table;
            }
            logger.error("build table fail: {}", table.getDigest());
            return null;
        }
    }
}
