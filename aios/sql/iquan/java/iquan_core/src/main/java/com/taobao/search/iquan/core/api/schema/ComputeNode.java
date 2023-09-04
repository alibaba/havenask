package com.taobao.search.iquan.core.api.schema;

public class ComputeNode {
    private String dbName;
    private Location location;

    public ComputeNode() {
    }

    public ComputeNode(String dbName, Location location) {
        this.dbName = dbName;
        this.location = location;
    }

    public boolean isSingle() {
        return 1 == location.getPartitionCnt();
    }

    public Location getLocation() {
        return location;
    }

    public void setLocation(Location location) {
        this.location = location;
    }

    public boolean isValid() {
        return location != null && location.isValid();
    }

    public String getDigest() {
        return String.format("location=[%s]", location.getDigest());
    }

    @Override
    public String toString() {
        return getDigest();
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }
}
