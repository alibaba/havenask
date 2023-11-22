package com.taobao.search.iquan.core.api.schema;

import com.taobao.search.iquan.client.common.json.catalog.IquanLocation;
import lombok.Getter;

@Getter
public class Location {
    private final String nodeName;
    private final int partitionCnt;

    public static final Location DEFAULT_QRS = new Location( "qrs", 1);
    public static final Location UNKNOWN = new Location( "unknown", Integer.MAX_VALUE);

    public Location(String nodeName, int partitionCnt) {
        this.nodeName = nodeName;
        this.partitionCnt = partitionCnt;
    }

    public Location(IquanLocation iquanLocation) {
        this.nodeName = iquanLocation.getNodeName();
        this.partitionCnt = iquanLocation.getPartitionCnt();
    }

    public boolean isSingle() {
        return 1 == partitionCnt;
    }

    public boolean isValid() {
        return !nodeName.isEmpty();
    }

    public String getDigest() {
        return String.format("tableGroupName=%s, partitionCnt=%d", nodeName, partitionCnt);
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

        return nodeName.equals(other.nodeName) && partitionCnt == other.partitionCnt;
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
