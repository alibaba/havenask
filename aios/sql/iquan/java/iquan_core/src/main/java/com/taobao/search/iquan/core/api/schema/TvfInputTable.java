package com.taobao.search.iquan.core.api.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.taobao.search.iquan.core.utils.IquanTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TvfInputTable {
    private static final Logger logger = LoggerFactory.getLogger(TvfInputTable.class);

    private String digest;
    private String name = "";
    private boolean autoInfer = true;
    private List<AbstractField> inputFields = new ArrayList<>();
    private List<AbstractField> checkFields = new ArrayList<>();

    // for internal use
    private List<RelDataTypeField> inputRelFields = null;
    private List<RelDataTypeField> checkRelFields = null;

    public TvfInputTable() {
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return getDigest();
    }

    public String getDigest() {
        return digest;
    }

    public void setDigest(String digest) {
        this.digest = digest;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isAutoInfer() {
        return autoInfer;
    }

    public void setAutoInfer(boolean autoInfer) {
        this.autoInfer = autoInfer;
    }

    public List<AbstractField> getInputFields() {
        return inputFields;
    }

    public void setInputFields(List<AbstractField> inputFields) {
        this.inputFields = inputFields;
    }

    public List<AbstractField> getCheckFields() {
        return checkFields;
    }

    public void setCheckFields(List<AbstractField> checkFields) {
        this.checkFields = checkFields;
    }

    public List<RelDataTypeField> getInputRelFields() {
        return inputRelFields;
    }

    public List<RelDataTypeField> getCheckRelFields() {
        return checkRelFields;
    }

    public boolean isValid() {
        if (!autoInfer && inputFields.isEmpty()) {
            logger.error("tvf input table valid fail: input fields is empty when not enable auto inference");
            return false;
        }

        inputRelFields = IquanTypeFactory.DEFAULT.createRelTypeFieldList(inputFields);
        checkRelFields = IquanTypeFactory.DEFAULT.createRelTypeFieldList(checkFields);

        if (inputRelFields == null
                || checkRelFields == null) {
            logger.error("tvf input table valid fail: convert AbstractField to RelDataTypeField fail");
            return false;
        }
        return true;
    }

    private void calcDigest() {
        StringBuilder sb = new StringBuilder();

        sb.append("TABLE");
        if (autoInfer) {
            sb.append("_auto");

            List<String> checkFieldsContent = checkFields.stream().map(v -> v.getDigest()).collect(Collectors.toList());
            sb.append("{").append(String.join(",", checkFieldsContent)).append("}");
        } else {
            List<String> inputFieldsContent = inputFields.stream().map(v -> v.getDigest()).collect(Collectors.toList());
            sb.append("{").append(String.join(",", inputFieldsContent)).append("}");
        }

        setDigest(sb.toString());
    }

    public static class Builder {
        private TvfInputTable inputTable;

        public Builder() {
            inputTable = new TvfInputTable();
        }

        public Builder name(String name) {
            inputTable.setName(name);
            return this;
        }

        public Builder autoInfer(boolean autoInfer) {
            inputTable.setAutoInfer(autoInfer);
            return this;
        }

        public Builder inputFields(List<AbstractField> inputFields) {
            inputTable.setInputFields(inputFields);
            return this;
        }

        public Builder checkFields(List<AbstractField> checkFields) {
            inputTable.setCheckFields(checkFields);
            return this;
        }

        public TvfInputTable build() {
            if (inputTable.getDigest() == null || inputTable.getDigest().isEmpty()) {
                inputTable.calcDigest();
            }
            if (inputTable.isValid()) {
                return inputTable;
            }
            logger.error("TvfInputTable.Builder: build fail for {}", inputTable.getDigest());
            return null;
        }
    }
}
