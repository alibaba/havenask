package com.taobao.search.iquan.client.common.service;

import java.util.List;

import com.taobao.search.iquan.client.common.json.catalog.IquanLocation;
import com.taobao.search.iquan.client.common.response.SqlResponse;
import com.taobao.search.iquan.client.common.utils.ErrorUtils;
import com.taobao.search.iquan.core.api.exception.LocationNodeAlreadyExistException;
import com.taobao.search.iquan.core.catalog.GlobalCatalog;

public class LocationService {
    public static SqlResponse registerLocations(GlobalCatalog catalog, List<IquanLocation> locations) {
        SqlResponse response = new SqlResponse();

        for (IquanLocation iquanLocation : locations) {
            try {
                catalog.registerLocation(iquanLocation);
            } catch (LocationNodeAlreadyExistException e) {
                ErrorUtils.setException(e, response);
            }
        }

        return response;
    }
}
