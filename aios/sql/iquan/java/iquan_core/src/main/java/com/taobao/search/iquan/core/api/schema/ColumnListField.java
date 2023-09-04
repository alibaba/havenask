package com.taobao.search.iquan.core.api.schema;

public class ColumnListField extends AbstractField {

    public ColumnListField() {
        super("", FieldType.FT_COLUMN_LIST, false);
    }

    public ColumnListField(String fieldName) {
        super(fieldName, FieldType.FT_COLUMN_LIST, false);
    }
}
