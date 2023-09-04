package com.taobao.search.iquan.core.api.schema;

public class MapField extends AbstractField {
    private AbstractField keyField;
    private AbstractField valueField;

    public MapField(String fieldName) {
        super(fieldName, FieldType.FT_MAP, false);
    }

    public MapField(String fieldName, AbstractField keyField, AbstractField valueField) {
        super(fieldName, FieldType.FT_MAP, false);
        this.keyField = keyField;
        this.valueField = valueField;
    }

    public AbstractField getKeyField() {
        return keyField;
    }

    public void setKeyField(AbstractField keyField) {
        this.keyField = keyField;
    }

    public AbstractField getValueField() {
        return valueField;
    }

    public void setValueField(AbstractField valueField) {
        this.valueField = valueField;
    }

    @Override
    public String getDigest() {
        return super.getDigest() + "|{" + keyField.getDigest() + "," + valueField.getDigest() + "}";
    }

    @Override
    public String toString() {
        return getDigest();
    }

    @Override
    public boolean isValid() {
        return super.isValid()
                && keyField != null && keyField.isValid() && keyField.isAtomicType()
                && valueField != null && valueField.isValid();
    }
}
