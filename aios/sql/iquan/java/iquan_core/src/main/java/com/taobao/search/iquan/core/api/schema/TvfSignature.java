package com.taobao.search.iquan.core.api.schema;

import com.taobao.search.iquan.core.utils.IquanTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TvfSignature {

    public enum Version {
        /**
         * 支持的调用形式
         * tvfFunc('arg0', arg1, (TABLE t1))
         * */
        VER_1_0,
        /**
         * 对比VER_1_0
         * 1. 将表输入的参数调整到了参数列表的最前面
         * 2. 新增按参数名指定参数值的传参方式
         * 支持的调用形式
         * tvfFunc(TABLE t1, 'arg1', arg2)
         * tvfFunc(
         *     data => TABLE t1,
         *     arg1 => 'arg1',
         *     arg2 => arg2
         * )
         * */
        VER_2_0
    }

    private static final Logger logger = LoggerFactory.getLogger(TvfSignature.class);

    private String digest;
    private Version version;
    private List<AbstractField> inputScalars = new ArrayList<>();
    private List<TvfInputTable> inputTables = new ArrayList<>();
    private List<AbstractField> outputNewFields = new ArrayList<>();
    private List<TvfOutputTable> outputTables = new ArrayList<>();

    // for internal use
    private List<RelDataType> inputRelScalars = null;
    private List<RelDataTypeField> outputRelNewFields = null;

    public TvfSignature() {
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

    public Version getVersion() {
        return version;
    }

    public void setVersion(Version version) {
        this.version = version;
    }

    public List<AbstractField> getInputScalars() {
        return inputScalars;
    }

    public void setInputScalars(List<AbstractField> inputScalars) {
        this.inputScalars = inputScalars;
    }

    public List<TvfInputTable> getInputTables() {
        return inputTables;
    }

    public void setInputTables(List<TvfInputTable> inputTables) {
        this.inputTables = inputTables;
    }

    public List<TvfOutputTable> getOutputTables() {
        return outputTables;
    }

    public void setOutputTables(List<TvfOutputTable> outputTables) {
        this.outputTables = outputTables;
    }

    public List<AbstractField> getOutputNewFields() {
        return outputNewFields;
    }

    public void setOutputNewFields(List<AbstractField> outputNewFields) {
        this.outputNewFields = outputNewFields;
    }

    public List<RelDataType> getInputRelScalars() {
        return inputRelScalars;
    }

    public List<RelDataTypeField> getOutputRelNewFields() {
        return outputRelNewFields;
    }

    public boolean isValid() {
        if (outputTables == null || outputTables.isEmpty()) {
            logger.error("tvf signature valid fail: output tables is null or empty");
            logger.error("tvf signature valid fail: {}", getDigest());
            return false;
        }

        if (inputScalars.stream().anyMatch(v -> !v.isValid())
                || inputTables.stream().anyMatch(v -> !v.isValid())
                || outputTables.stream().anyMatch(v -> !v.isValid())
                || outputNewFields.stream().anyMatch(v -> !v.isValid())) {
            logger.error("tvf signature valid fail: {}", getDigest());
            return false;
        }

        inputRelScalars = IquanTypeFactory.DEFAULT.createRelTypeList(inputScalars);
        outputRelNewFields = IquanTypeFactory.DEFAULT.createRelTypeFieldList(outputNewFields);

        if (inputRelScalars == null
                || outputRelNewFields == null) {
            logger.error("tvf signature valid fail: convert AbstractField to RelDataType fail");
            return false;
        }
        return true;
    }

    private void calcDigest(String name) {
        StringBuilder sb = new StringBuilder(256);
        List<String> outputNewFieldsContent = outputNewFields.stream().map(v -> v.getDigest()).collect(Collectors.toList());
        sb.append("(").append(String.join(",", outputNewFieldsContent));

        List<String> outputTablesContent = outputTables.stream().map(v -> v.getDigest()).collect(Collectors.toList());
        if (!outputTablesContent.isEmpty()) {
            sb.append(",");
        }
        sb.append(String.join(",", outputTablesContent));
        sb.append(")");

        sb.append("TVF_").append(name);

        List<String> inputScalarsContent = inputScalars.stream().map(v -> v.getDigest()).collect(Collectors.toList());
        sb.append("(").append(String.join(",", inputScalarsContent));

        List<String> inputTablesContent = inputTables.stream().map(v -> v.getDigest()).collect(Collectors.toList());
        if (!inputTablesContent.isEmpty()) {
            sb.append(",");
        }
        sb.append(String.join(",", inputTablesContent));
        sb.append(")");

        setDigest(sb.toString());
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private TvfSignature signature;

        public Builder() {
            signature = new TvfSignature();
        }

        public Builder version(Version version) {
            signature.setVersion(version);
            return this;
        }

        public Builder inputParams(List<AbstractField> inputParams) {
            signature.setInputScalars(inputParams);
            return this;
        }

        public Builder inputTables(List<TvfInputTable> inputTables) {
            signature.setInputTables(inputTables);
            return this;
        }

        public Builder outputTables(List<TvfOutputTable> outputTables) {
            signature.setOutputTables(outputTables);
            return this;
        }

        public Builder outputNewFields(List<AbstractField> outputNewFields) {
            signature.setOutputNewFields(outputNewFields);
            return this;
        }

        public TvfSignature build(String name) {
            if (signature.getDigest() == null || signature.getDigest().isEmpty()) {
                signature.calcDigest(name);
            }
            if (signature.isValid()) {
                return signature;
            }
            logger.error("TvfSignature.Builder: build fail for {}", signature.getDigest());
            return null;
        }
    }
}
