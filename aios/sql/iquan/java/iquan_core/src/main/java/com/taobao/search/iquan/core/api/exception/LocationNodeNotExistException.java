package com.taobao.search.iquan.core.api.exception;

public class LocationNodeNotExistException extends Exception {
    private static final String MSG = "LocationNode %s does not exist in Catalog %s.";

    public LocationNodeNotExistException(String catalogName, String locationName) {
        this(catalogName, locationName, null);
    }

    public LocationNodeNotExistException(String catalogName, String locationName, Throwable cause) {
        super(String.format(MSG, locationName, catalogName), cause);
    }
}
