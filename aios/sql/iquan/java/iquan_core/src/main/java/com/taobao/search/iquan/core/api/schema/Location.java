package com.taobao.search.iquan.core.api.schema;

public class Location {
    private final String tableGroupName;
    private final int partitionCnt;

    public static final Location DEFAULT_QRS = new Location( "qrs", 1);
    public static final Location UNKNOWN = new Location( "unknown", Integer.MAX_VALUE);

    public Location(String tableGroupName, int partitionCnt) {
        this.tableGroupName = tableGroupName;
        this.partitionCnt = partitionCnt;
    }

    public String getTableGroupName() {
        return tableGroupName;
    }

    @Deprecated //partitionCnt is to be corrected in future suez catalog
    public int getPartitionCnt() {
        return partitionCnt;
    }

    public boolean isValid() {
        return !tableGroupName.isEmpty();
    }

    public String getDigest() {
        return String.format("tableGroupName=%s, partitionCnt=%d", tableGroupName, partitionCnt);
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof Location)) {
            return false;
        }
        Location other = (Location) object;
        if (this == other) {
            return true;
        }
        /*
        return dbName.equals(other.dbName)
                && tableGroupName.equals(other.tableGroupName)
                && partitionCnt == other.partitionCnt;
        */
        return tableGroupName.equals(other.tableGroupName);
    }

    /*
    @Override
    public int hashCode() {
        return Objects.hash(dbName, tableGroupName, partitionCnt);
    }
    */

    @Override
    public String toString() {
        return getDigest();
    }
}
