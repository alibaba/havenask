package com.taobao.search.iquan.client.common.json.common;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.taobao.search.iquan.client.common.common.ConstantDefine;

public class JsonLocation {
    @JsonProperty("table_group_name")
    private String tableGroupName = ConstantDefine.EMPTY;

    @JsonProperty("partition_cnt")
    private int partitionCnt = 0;

    @JsonProperty("service_desc")
    private String serviceDesc = ConstantDefine.EMPTY;

    public JsonLocation() {
    }

    public String getTableGroupName() {
        return tableGroupName;
    }

    public void setTableGroupName(String tableGroupName) {
        this.tableGroupName = tableGroupName;
    }

    @Deprecated //partitionCnt is to be corrected in future suez catalog
    public int getPartitionCnt() {
        return partitionCnt;
    }

    public void setPartitionCnt(int partitionCnt) {
        this.partitionCnt = partitionCnt;
    }

    public String getServiceDesc() {
        return serviceDesc;
    }

    public void setServiceDesc(String serviceDesc) {
        this.serviceDesc = serviceDesc;
    }

    @JsonIgnore
    public boolean isValid() {
        return !tableGroupName.isEmpty();
    }
}
