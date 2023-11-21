package com.taobao.search.iquan.core.api.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class RowField extends AbstractField {
    private List<AbstractField> fields;

    public RowField(String fieldName) {
        super(fieldName, FieldType.FT_ROW, false);
        fields = new ArrayList<>();
    }

    public RowField(String fieldName, List<AbstractField> fields) {
        super(fieldName, FieldType.FT_ROW, false);
        this.fields = fields;
    }

    public int getFieldSize() {
        return fields.size();
    }

    public List<AbstractField> getFields() {
        return fields;
    }

    public void setFields(List<AbstractField> fields) {
        this.fields.addAll(fields);
    }

    public AbstractField getField(int index) {
        return fields.get(index);
    }

    public void addField(AbstractField field) {
        fields.add(field);
    }

    @Override
    public String getDigest() {
        List<String> strList = fields.stream().map(v -> v.getDigest()).collect(Collectors.toList());
        return super.getDigest() + "|[" + String.join(",", strList) + "]";
    }

    @Override
    public String toString() {
        return getDigest();
    }

    @Override
    public boolean isValid() {
        return super.isValid() && !fields.isEmpty() && fields.stream().allMatch(v -> v.isValid());
    }
}
