package com.taobao.search.iquan.client.common.json.api;

import com.fasterxml.jackson.annotation.JsonProperty;

public class JsonKMonConfig {
    @JsonProperty("service_name")
    public String serviceName = "kmon.sql"; // 指标前缀，如kmon.sql.xxx

    @JsonProperty("tenant_name")
    public String tenantName = "default"; // 租户，默认为default

    @JsonProperty("flume_address")
    public String flumeAddress = "127.0.0.1:4141";

    @JsonProperty("auto_recycle")
    public boolean autoRecycle = true;

    @JsonProperty("global_tags")
    public String globalTags = null;

    public boolean isValid() {
        return !serviceName.isEmpty()
                && !tenantName.isEmpty()
                && !flumeAddress.isEmpty();
    }
}
