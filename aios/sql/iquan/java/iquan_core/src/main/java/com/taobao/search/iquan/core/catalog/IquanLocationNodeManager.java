package com.taobao.search.iquan.core.catalog;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.taobao.search.iquan.client.common.json.catalog.IquanLocation;
import com.taobao.search.iquan.client.common.json.catalog.JsonTableIdentity;
import com.taobao.search.iquan.client.common.service.TableService;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.CatalogException;
import com.taobao.search.iquan.core.api.exception.DatabaseNotExistException;
import com.taobao.search.iquan.core.api.exception.LocationNodeAlreadyExistException;
import com.taobao.search.iquan.core.api.exception.LocationNodeNotExistException;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import com.taobao.search.iquan.core.api.exception.TableAlreadyExistException;
import com.taobao.search.iquan.core.api.exception.TableNotExistException;
import com.taobao.search.iquan.core.api.schema.Location;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IquanLocationNodeManager {
    private static final Logger logger = LoggerFactory.getLogger(IquanLocationNodeManager.class);

    @Getter
    private final String catalogName;
    @Getter
    private final Map<String, IquanLocation> locationMap = new HashMap<>();
    private final Map<JsonTableIdentity, List<IquanLocation>> tableToLocation = new HashMap<>();
    @Getter
    private final List<Location> qrsLocations = new ArrayList<>();
    @Getter
    private final List<Location> searcherLocations = new ArrayList<>();
    @Getter
    private final List<Location> computeNodeLocation = new ArrayList<>();
    public IquanLocationNodeManager(String catalogName) {
        this.catalogName = catalogName;
    }

    public void registerLocation(IquanLocation location) throws LocationNodeAlreadyExistException {
        if (location == null) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_CATALOG,
                    String.format("invalid LocationNode register rejected in catalog [%s]", catalogName));
        }
        String nodeName = location.getNodeName();
        if (locationMap.containsKey(nodeName)) {
            throw new LocationNodeAlreadyExistException(catalogName, nodeName);
        }
        locationMap.put(nodeName, location);
        logger.info("register location {} {} success", catalogName, nodeName);
    }

    public IquanLocation getLocation(String nodeName) throws LocationNodeNotExistException {
        if (!locationMap.containsKey(nodeName)) {
            throw new LocationNodeNotExistException(catalogName, nodeName);
        }

        return locationMap.get(nodeName);
    }

    public List<IquanLocation> getLocationByTable(JsonTableIdentity jsonTableIdentity) {
        return tableToLocation.getOrDefault(jsonTableIdentity, Collections.EMPTY_LIST);
    }

    public void init(GlobalCatalog catalog)
            throws TableAlreadyExistException, TableNotExistException, DatabaseNotExistException {
        for (IquanLocation location : locationMap.values()) {
            location.init();
        }
        registerMainAuxJoinTable(catalog);
        initTableToLocations();
        initClassifyLocations();
    }

    private void initTableToLocations() {
        for (IquanLocation location : locationMap.values()) {
            for (JsonTableIdentity jsonTableIdentity : location.getTableIdentities()) {
                if (!tableToLocation.containsKey(jsonTableIdentity)) {
                    tableToLocation.put(jsonTableIdentity, new ArrayList<>());
                }
                List<IquanLocation> locations = tableToLocation.get(jsonTableIdentity);
                locations.add(location);
            }
        }
    }

    private void initClassifyLocations() {
        for (IquanLocation iquanLocation : locationMap.values()) {
            if (StringUtils.equals("qrs", iquanLocation.getNodeType())) {
                qrsLocations.add(new Location(iquanLocation));
            } else if (StringUtils.equals("compute_node", iquanLocation.getNodeType())) {
                computeNodeLocation.add(new Location(iquanLocation));
            } else if (StringUtils.equals("searcher", iquanLocation.getNodeType())) {
                searcherLocations.add(new Location(iquanLocation));
            } else {
                throw new CatalogException(
                        String.format(
                                "Location [%s]'s node type [%s] is invalid, only support qrs, searcher and compute_node",
                                iquanLocation,
                                iquanLocation.getNodeType()));
            }
        }
    }

    public void registerMainAuxJoinTable(GlobalCatalog catalog)
            throws TableAlreadyExistException, TableNotExistException, DatabaseNotExistException {
        for(IquanLocation location : locationMap.values()) {
            TableService.registerMainAuxTables(catalog, location);
        }
    }
}
