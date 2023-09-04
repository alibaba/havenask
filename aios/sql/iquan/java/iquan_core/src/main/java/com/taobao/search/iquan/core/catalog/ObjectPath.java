package com.taobao.search.iquan.core.catalog;

import java.io.Serializable;
import java.util.Objects;

public class ObjectPath  implements Serializable {
    private final String databaseName;
    private final String objectName;

    public ObjectPath(String databaseName, String objectName) {
        assert !isNullOrWhitespaceOnly(databaseName) :
                "databaseName cannot be null or empty";
        assert !isNullOrWhitespaceOnly(objectName) :
                "objectName cannot be null or empty";

        this.databaseName = databaseName;
        this.objectName = objectName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getObjectName() {
        return objectName;
    }

    public String getFullName() {
        return String.format("%s.%s", databaseName, objectName);
    }

    public static ObjectPath fromString(String fullName) {
        assert !isNullOrWhitespaceOnly(fullName) : "fullName cannot be null or empty";

        String[] paths = fullName.split("\\.");

        if (paths.length != 2) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot get split '%s' to get databaseName and objectName", fullName));
        }

        return new ObjectPath(paths[0], paths[1]);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ObjectPath that = (ObjectPath) o;

        return Objects.equals(databaseName, that.databaseName)
                && Objects.equals(objectName, that.objectName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, objectName);
    }

    @Override
    public String toString() {
        return String.format("%s.%s", databaseName, objectName);
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
}
