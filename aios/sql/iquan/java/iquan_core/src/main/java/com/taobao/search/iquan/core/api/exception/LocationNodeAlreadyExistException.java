package com.taobao.search.iquan.core.api.exception;

public class LocationNodeAlreadyExistException extends Exception{
    private static final String MSG = "LocationNode %s already exists in Catalog %s.";

    public LocationNodeAlreadyExistException(String catalogName, String locationName) {
        this(catalogName, locationName, null);
    }

    public LocationNodeAlreadyExistException(String catalogName, String locationName, Throwable cause) {
        super(String.format(MSG, locationName, catalogName), cause);
    }
}
