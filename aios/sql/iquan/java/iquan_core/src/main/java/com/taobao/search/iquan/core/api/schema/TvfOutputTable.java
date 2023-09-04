package com.taobao.search.iquan.core.api.schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class TvfOutputTable {
    private static final Logger logger = LoggerFactory.getLogger(TvfOutputTable.class);

    private boolean autoInfer = false;
    private String digest;
    private List<String> inputFields = new ArrayList<>();

    public TvfOutputTable() {
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

    public boolean isAutoInfer() {
        return autoInfer;
    }

    public void setAutoInfer(boolean autoInfer) {
        this.autoInfer = autoInfer;
    }

    public List<String> getInputFields() {
        return inputFields;
    }

    public void setInputFields(List<String> inputFields) {
        this.inputFields = inputFields;
    }

    public boolean isValid() {
        return true;
    }

    public void calcDigest() {
        StringBuilder sb = new StringBuilder();

        sb.append("TABLE");
        if (autoInfer) {
            sb.append("_auto{}");
        } else {
            sb.append("{").append(String.join(",", inputFields)).append("}");
        }

        setDigest(sb.toString());
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private TvfOutputTable outputTable;

        public Builder() {
            outputTable = new TvfOutputTable();
        }

        public Builder autoInfer(boolean autoInfer) {
            outputTable.setAutoInfer(autoInfer);
            return this;
        }

        public Builder inputFields(List<String> inputFields) {
            outputTable.setInputFields(inputFields);
            return this;
        }

        public TvfOutputTable build() {
            if (outputTable.getDigest() == null || outputTable.getDigest().isEmpty()) {
                outputTable.calcDigest();
            }
            if (outputTable.isValid()) {
                return outputTable;
            }

            logger.error("TvfOutputTable.Builder: build fail for {}", outputTable.getDigest());
            return null;
        }
    }
}
