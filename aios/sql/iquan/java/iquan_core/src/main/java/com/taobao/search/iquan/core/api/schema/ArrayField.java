package com.taobao.search.iquan.core.api.schema;

public class ArrayField extends AbstractField {
    private AbstractField elementField;

    public ArrayField(AbstractField elementField) {
        super(elementField.getFieldName(), FieldType.FT_ARRAY, elementField.getIsAttribute());
        this.elementField = elementField;
    }

    public ArrayField(String fieldName, FieldType atomicFieldType, boolean isAttribute) {
        super(fieldName, FieldType.FT_ARRAY, isAttribute);
        if (!atomicFieldType.isAtomicType()) {
            throw new UnsupportedOperationException(
                    String.format("create array field {} fail, not support type {} for element field.",
                            fieldName, atomicFieldType.toString())
            );
        }
        this.elementField = new AtomicField(fieldName, atomicFieldType, isAttribute);
    }

    public ArrayField(String fieldName, String atomicFieldType, boolean isAttribute) {
        this(fieldName, FieldType.from(atomicFieldType), isAttribute);
    }

    public AbstractField getElementField() {
        return elementField;
    }

    public void setElementField(AbstractField elementField) {
        this.elementField = elementField;
    }

    @Override
    public boolean isValid() {
        return super.isValid() && (elementField != null) && elementField.isValid();
    }

    @Override
    public String getDigest() {
        return super.getDigest() + "|" + elementField.getDigest();
    }

    @Override
    public String toString() {
        return getDigest();
    }
}
