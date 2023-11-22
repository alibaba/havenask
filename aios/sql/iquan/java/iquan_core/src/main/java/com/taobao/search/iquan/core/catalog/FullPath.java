package com.taobao.search.iquan.core.catalog;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class FullPath implements Serializable {
    private String catalogName;
    private String databaseName;
    private String objectName;

    public FullPath(String catalogName, String databaseName, String objectName) {
        assert !isNullOrWhitespaceOnly(catalogName) : "catalogName cannot be null or empty";
        assert !isNullOrWhitespaceOnly(databaseName) : "databaseName cannot be null or empty";
        assert !isNullOrWhitespaceOnly(objectName) : "objectName cannot be null or empty";

        this.catalogName=catalogName;
        this.databaseName=databaseName;
        this.objectName=objectName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FullPath that = (FullPath) o;

        return Objects.equals(databaseName, that.databaseName)
            && Objects.equals(objectName, that.objectName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, objectName);
    }

    @Override
    public String toString() {
        return String.format("%s.%s.%s", catalogName, databaseName, objectName);
    }

    public static boolean isNullOrWhitespaceOnly(String str) {
        if (str != null && str.length() != 0) {
            for(int i = 0; i < str.length(); ++i) {
                if (!Character.isWhitespace(str.charAt(i))) {
                    return false;
                }
            }
        }
        return true;
    }

    public List<String> toList() {
        return Arrays.asList(catalogName, databaseName, objectName);
    }
}
