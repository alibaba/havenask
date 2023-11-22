package com.taobao.search.iquan.core.api.schema;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.taobao.search.iquan.core.api.exception.ExceptionUtils;
import com.taobao.search.iquan.core.api.exception.IquanNotValidateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractField {
    private static final Logger logger = LoggerFactory.getLogger(AbstractField.class);

    protected String fieldName;
    protected FieldType fieldType;
    protected Object defaultValue;
    protected boolean isAttribute;
    protected Map<String, String> extendInfos;

    protected AbstractField() {
        this("", FieldType.FT_INVALID, false, new HashMap<>(4));
    }

    protected AbstractField(FieldType fieldType, boolean isAttribute) {
        this("", fieldType, isAttribute, new HashMap<>(4));
    }

    protected AbstractField(String fieldName, FieldType fieldType, boolean isAttribute) {
        this(fieldName, fieldType, isAttribute, new HashMap<>(4));
    }

    protected AbstractField(String fieldName, FieldType fieldType, boolean isAttribute, Map<String, String> extendInfos) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.isAttribute = isAttribute;
        this.extendInfos = Objects.requireNonNull(extendInfos);
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public FieldType getFieldType() {
        return fieldType;
    }

    public void setFieldType(FieldType fieldType) {
        this.fieldType = fieldType;
    }

    public Object getDefaultValue() {
        return defaultValue == null ? fieldType.defaultValue() : defaultValue;
    }

    public void setDefaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
    }

    public boolean getIsAttribute() {
        return isAttribute;
    }

    public void setIsAttribute(boolean isAttribute) {
        this.isAttribute = isAttribute;
    }

    public Map<String, String> getExtendInfos() {
        return extendInfos;
    }

    public void setExtendInfos(Map<String, String> extendInfos) {
        this.extendInfos = extendInfos;
    }

    public String getDigest() {
        StringBuffer sb = new StringBuffer(64);
        if (fieldName.isEmpty()) {
            sb.append(fieldType.getName());
        } else {
            sb.append(fieldName).append("|").append(fieldType.getName());
        }
        sb.append("|").append(isAttribute);
        if (!extendInfos.isEmpty()) {
            sb.append(extendInfos.toString());
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return getDigest();
    }

    public boolean isValid() {
        boolean isValid = true;
        try {
            ExceptionUtils.throwIfTrue(fieldName == null, "fieldName is null");
            ExceptionUtils.throwIfTrue(fieldType == null, "fieldType is null");
            ExceptionUtils.throwIfTrue(extendInfos == null, "fieldType is null");
            ExceptionUtils.throwIfTrue(!fieldType.isValid(),
                    "fieldType is not valid");
        } catch (IquanNotValidateException e) {
            logger.error(e.getMessage());
            isValid = false;
        }
        return isValid;
    }

    public boolean isAtomicType() {
        return fieldType.isAtomicType();
    }

    public boolean isRowType() {
        return fieldType.isRowType();
    }

    public boolean isArrayType() {
        return fieldType == FieldType.FT_ARRAY;
    }

    public boolean isMultiSetType() {
        return fieldType == FieldType.FT_MULTISET;
    }

    public boolean isMapType() {
        return fieldType == FieldType.FT_MAP;
    }

    public boolean isColumnListType() {
        return fieldType == FieldType.FT_COLUMN_LIST;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractField other = (AbstractField) o;
        return fieldName.equals(other.fieldName)
                && (fieldType == other.fieldType)
                && (isAttribute == other.isAttribute);
    }
}
