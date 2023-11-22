package com.taobao.search.iquan.client.common.json.catalog;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.taobao.search.iquan.core.common.ConstantDefine;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@AllArgsConstructor
@NoArgsConstructor
public class JsonCatalog {
    @JsonProperty(value = ConstantDefine.CATALOG_NAME, required = true)
    private String catalogName;

    @JsonProperty(value = ConstantDefine.DATABASES, required = true)
    private List<JsonDatabase> databases;

    @JsonProperty(value = ConstantDefine.LOCATIONS, required = true)
    private List<IquanLocation> locations;

    public boolean isValid() {
        return catalogName != null && !databases.isEmpty();
    }
}
