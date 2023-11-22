package com.taobao.search.iquan.core.utils;

import java.util.ArrayList;
import java.util.List;

import com.taobao.search.iquan.core.api.schema.AbstractField;
import com.taobao.search.iquan.core.api.schema.ArrayField;
import com.taobao.search.iquan.core.api.schema.FieldType;
import com.taobao.search.iquan.core.api.schema.MapField;
import com.taobao.search.iquan.core.api.schema.MultiSetField;
import com.taobao.search.iquan.core.api.schema.RowField;
import com.taobao.search.iquan.core.common.ConstantDefine;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IquanTypeFactory {
    public final static IquanTypeFactory DEFAULT = IquanTypeFactory.builder().build();
    protected static final Logger logger = LoggerFactory.getLogger(IquanTypeFactory.class);
    protected boolean scalarTypeNullable = false;
    protected boolean compoundTypeNullable = false;
    protected RelDataTypeFactory relTypeFactory;

    public IquanTypeFactory() {
        this.relTypeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    }

    public IquanTypeFactory(RelDataTypeFactory relTypeFactory) {
        this.relTypeFactory = relTypeFactory;
    }

    public static FieldType createFieldType(SqlTypeName type) {
        switch (type) {
            case BOOLEAN:
                return FieldType.FT_BOOLEAN;
            case TINYINT:
                return FieldType.FT_INT8;
            case SMALLINT:
                return FieldType.FT_INT16;
            case INTEGER:
                return FieldType.FT_INT32;
            case BIGINT:
                return FieldType.FT_INT64;
            case FLOAT:
                return FieldType.FT_FLOAT;
            case DOUBLE:
                return FieldType.FT_DOUBLE;
            case VARCHAR:
            case CHAR:
                return FieldType.FT_STRING;
            case DATE:
                return FieldType.FT_DATE;
            case TIME:
                return FieldType.FT_TIME;
            case TIMESTAMP:
                return FieldType.FT_TIMESTAMP;
            case COLUMN_LIST:
                return FieldType.FT_COLUMN_LIST;
            default:
                logger.error("createFieldType fail: type {} is not support", type);
                return FieldType.FT_INVALID;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public boolean isScalarTypeNullable() {
        return scalarTypeNullable;
    }

    public void setScalarTypeNullable(boolean scalarTypeNullable) {
        this.scalarTypeNullable = scalarTypeNullable;
    }

    public boolean isCompoundTypeNullable() {
        return compoundTypeNullable;
    }

    public void setCompoundTypeNullable(boolean compoundTypeNullable) {
        this.compoundTypeNullable = compoundTypeNullable;
    }

    public RelDataTypeFactory getRelTypeFactory() {
        return relTypeFactory;
    }

    public void setRelTypeFactory(RelDataTypeFactory relTypeFactory) {
        this.relTypeFactory = relTypeFactory;
    }

    private RelDataType createRelTypeForAtomic(AbstractField field) {
        assert field.isAtomicType();
        SqlTypeName sqlTypeName;
        switch (field.getFieldType()) {
            case FT_BOOLEAN:
                sqlTypeName = SqlTypeName.BOOLEAN;
                break;
            case FT_INT8:
                sqlTypeName = SqlTypeName.TINYINT;
                break;
            case FT_INT16:
                sqlTypeName = SqlTypeName.SMALLINT;
                break;
            case FT_INT32:
                sqlTypeName = SqlTypeName.INTEGER;
                break;
            case FT_INT64:
                sqlTypeName = SqlTypeName.BIGINT;
                break;
            case FT_UINT8:
            case FT_UINT16:
                sqlTypeName = SqlTypeName.INTEGER;
                break;
            case FT_UINT32:
            case FT_UINT64:
                sqlTypeName = SqlTypeName.BIGINT;
                break;
            case FT_FLOAT:
                sqlTypeName = SqlTypeName.FLOAT;
                break;
            case FT_DOUBLE:
                sqlTypeName = SqlTypeName.DOUBLE;
                break;
            case FT_STRING:
                sqlTypeName = SqlTypeName.VARCHAR;
                break;
            case FT_DATE:
                sqlTypeName = SqlTypeName.DATE;
                break;
            case FT_TIME:
                sqlTypeName = SqlTypeName.TIME;
                break;
            case FT_TIMESTAMP:
                sqlTypeName = SqlTypeName.TIMESTAMP;
                break;
            case FT_ANY:
                sqlTypeName = SqlTypeName.ANY;
                break;
            case FT_COLUMN_LIST:
                sqlTypeName = SqlTypeName.COLUMN_LIST;
                break;
            default:
                sqlTypeName = null;
        }
        if (sqlTypeName == null) {
            logger.error("createRelTypeForAtomic fail: name {}, type {} is not support",
                    field.getFieldName(), field.getFieldType().getName());
            return null;
        }

        RelDataTypeSystem relTypeSystem = relTypeFactory.getTypeSystem();
        RelDataType atomicRelType;
        if (sqlTypeName.allowsPrecScale(true, false)) {
            atomicRelType = relTypeFactory.createSqlType(sqlTypeName, relTypeSystem.getMaxPrecision(sqlTypeName));
        } else {
            atomicRelType = relTypeFactory.createSqlType(sqlTypeName);
        }
        if (atomicRelType != null) {
            atomicRelType = relTypeFactory.createTypeWithNullability(atomicRelType, scalarTypeNullable);
        }
        return atomicRelType;
    }

    private RelDataType createRelTypeForRow(AbstractField field) {
        assert field.isRowType();
        RowField rowField = (RowField) field;
        List<AbstractField> recordFields = rowField.getFields();
        int recordFieldsSize = recordFields.size();

        List<RelDataType> typeList = new ArrayList<>(recordFieldsSize);
        List<String> fieldNameList = new ArrayList<>(recordFieldsSize);
        for (int i = 0; i < recordFieldsSize; ++i) {
            AbstractField innerField = recordFields.get(i);
            RelDataType innerRelType = createRelType(innerField);
            if (innerRelType == null) {
                logger.error("createRelTypeForRow fail: name {}, inner field name {}, inner field type {}",
                        field.getFieldName(), innerField.getFieldName(), innerField.getFieldType().getName());
                return null;
            }
            String name = innerField.getFieldName().isEmpty() ? ConstantDefine.DEFAULT_FIELD_NAME + i : innerField.getFieldName();

            typeList.add(innerRelType);
            fieldNameList.add(name);
        }

        RelDataType rowRelType = relTypeFactory.createStructType(StructKind.FULLY_QUALIFIED, typeList, fieldNameList);
        if (rowRelType != null) {
            rowRelType = relTypeFactory.createTypeWithNullability(rowRelType, compoundTypeNullable);
        }
        return rowRelType;
    }

    private RelDataType createRelTypeForArray(AbstractField field) {
        assert field.isArrayType();
        ArrayField arrayField = (ArrayField) field;
        AbstractField elementField = arrayField.getElementField();

        RelDataType elementRelType = createRelType(elementField);
        if (elementRelType == null) {
            logger.error("createRelTypeForArray fail: name {}, element type {}",
                    field.getFieldName(), elementField.getFieldType().getName());
        }

        RelDataType arrayRelType = relTypeFactory.createArrayType(elementRelType, -1);
        if (arrayRelType != null) {
            arrayRelType = relTypeFactory.createTypeWithNullability(arrayRelType, compoundTypeNullable);
        }
        return arrayRelType;
    }

    private RelDataType createRelTypeForMultiSet(AbstractField field) {
        assert field.isMultiSetType();
        MultiSetField multiSetField = (MultiSetField) field;
        AbstractField elementField = multiSetField.getElementField();

        RelDataType elementRelType = createRelType(elementField);
        if (elementRelType == null) {
            logger.error("createRelTypeForMultiSet fail: name {}, element type {}",
                    field.getFieldName(), elementField.getFieldType().getName());
        }

        RelDataType multisetRelType = relTypeFactory.createMultisetType(elementRelType, -1);
        if (multisetRelType != null) {
            multisetRelType = relTypeFactory.createTypeWithNullability(multisetRelType, compoundTypeNullable);
        }
        return multisetRelType;
    }

    private RelDataType createRelTypeForMap(AbstractField field) {
        assert field.isValid();
        MapField mapField = (MapField) field;
        AbstractField keyField = mapField.getKeyField();
        AbstractField valueField = mapField.getValueField();

        RelDataType keyRelType = createRelType(keyField);
        RelDataType valueRelType = createRelType(valueField);
        if (keyRelType == null || valueRelType == null) {
            logger.error("createRelTypeForMap fail: name {}, key type {}, value type {}",
                    field.getFieldName(), keyField.getFieldType().getName(), valueField.getFieldType().getName());
            return null;
        }

        RelDataType mapType = relTypeFactory.createMapType(keyRelType, valueRelType);
        if (mapType != null) {
            mapType = relTypeFactory.createTypeWithNullability(mapType, compoundTypeNullable);
        }
        return mapType;
    }

    private RelDataType createRelTypeForColumnList(AbstractField field) {
        return relTypeFactory.createSqlType(SqlTypeName.COLUMN_LIST);
    }

    public RelDataType createRelType(AbstractField field) {
        RelDataType relDataType = null;
        if (field.isAtomicType()) {
            relDataType = createRelTypeForAtomic(field);
        } else if (field.isRowType()) {
            relDataType = createRelTypeForRow(field);
        } else if (field.isArrayType()) {
            relDataType = createRelTypeForArray(field);
        } else if (field.isMultiSetType()) {
            relDataType = createRelTypeForMultiSet(field);
        } else if (field.isMapType()) {
            relDataType = createRelTypeForMap(field);
        } else if (field.isColumnListType()) {
            relDataType = createRelTypeForColumnList(field);
        }
        if (relDataType == null) {
            logger.error("createRelType fail: name {}, type {}", field.getFieldName(), field.getFieldType().getName());
            return null;
        }
        return relDataType;
    }

    public List<RelDataType> createRelTypeList(List<AbstractField> fieldList) {
        List<RelDataType> relTypeList = new ArrayList<>(fieldList.size());
        for (AbstractField field : fieldList) {
            RelDataType relType = createRelType(field);
            if (relType == null) {
                return null;
            }
            relTypeList.add(relType);
        }
        return relTypeList;
    }

    public RelDataType createStructRelType(List<AbstractField> fieldList) {
        if (fieldList.size() == 1) {
            return createRelType(fieldList.get(0));
        }

        RowField rowField = new RowField(ConstantDefine.DEFAULT_FIELD_NAME, fieldList);
        return createRelTypeForRow(rowField);
    }

    public List<RelDataTypeField> createRelTypeFieldList(List<AbstractField> fieldList) {
        List<RelDataTypeField> relTypeFieldList = new ArrayList<>(fieldList.size());
        int index = 0;
        for (AbstractField field : fieldList) {
            RelDataType relType = createRelType(field);
            if (relType == null) {
                return null;
            }
            String name = field.getFieldName().isEmpty() ? ConstantDefine.DEFAULT_FIELD_NAME + index : field.getFieldName();
            relTypeFieldList.add(new RelDataTypeFieldImpl(name, index++, relType));
        }
        return relTypeFieldList;
    }

    public static class Builder {
        private IquanTypeFactory typeFactory;

        public Builder() {
            typeFactory = new IquanTypeFactory();
        }

        public Builder scalarTypeNullable(boolean scalarTypeNullable) {
            typeFactory.setScalarTypeNullable(scalarTypeNullable);
            return this;
        }

        public Builder compoundTypeNullable(boolean compoundTypeNullable) {
            typeFactory.setCompoundTypeNullable(compoundTypeNullable);
            return this;
        }

        public Builder relTypeFactory(RelDataTypeFactory relTypeFactory) {
            typeFactory.setRelTypeFactory(relTypeFactory);
            return this;
        }

        public IquanTypeFactory build() {
            return typeFactory;
        }
    }
}
