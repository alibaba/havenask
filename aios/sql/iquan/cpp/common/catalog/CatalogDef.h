/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <string>
#include <vector>

#include "autil/legacy/jsonizable.h"
#include "iquan/common/catalog/DatabaseDef.h"
#include "iquan/common/catalog/LocationDef.h"
#include "sql/common/common.h"

namespace iquan {

class CatalogDef : public autil::legacy::Jsonizable {
public:
    CatalogDef() {
        database(sql::SQL_SYSTEM_DATABASE);
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("catalog_name", catalogName, catalogName);
        json.Jsonize("databases", databases, databases);
        json.Jsonize("locations", locations, locations);
    }

    bool isValid() const {
        for (const auto &database : databases) {
            if (!database.isValid()) {
                return false;
            }
        }

        for (const auto &location : locations) {
            if (!location.isValid()) {
                return false;
            }
        }

        return !catalogName.empty();
    }

    bool merge(CatalogDef &other) {
        if (other.catalogName != catalogName) {
            return false;
        }
        bool ret = true;
        for (auto &otherDatabase : other.databases) {
            auto &thisDatabase = database(otherDatabase.dbName);
            ret = ret && thisDatabase.merge(otherDatabase);
        }
        for (auto &otherLocation : other.locations) {
            auto &thisLocation = location(otherLocation.sign);
            ret = ret && thisLocation.merge(otherLocation);
        }
        return ret;
    }

    DatabaseDef &database(const std::string &dbName) {
        for (auto &database : databases) {
            if (database.dbName == dbName) {
                return database;
            }
        }
        databases.emplace_back(DatabaseDef());
        auto &database = databases.back();
        database.dbName = dbName;
        return database;
    }

    LocationDef &location(const LocationSign &sign) {
        for (auto &location : locations) {
            if (location.sign.nodeName == sign.nodeName) {
                return location;
            }
        }
        locations.emplace_back(LocationDef());
        auto &location = locations.back();
        location.sign = sign;
        return location;
    }

public:
    std::string catalogName;
    std::vector<DatabaseDef> databases;
    std::vector<LocationDef> locations;
};

class CatalogDefs : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("catalogs", catalogs, catalogs);
    }

    bool isValid() const {
        for (const auto &catalog : catalogs) {
            if (!catalog.isValid()) {
                return false;
            }
        }

        return !catalogs.empty();
    }

    bool merge(CatalogDefs &other) {
        bool ret = true;
        for (auto &otherCatalog : other.catalogs) {
            auto &thisCatalog = catalog(otherCatalog.catalogName);
            ret = ret && thisCatalog.merge(otherCatalog);
        }
        return ret;
    }

    CatalogDef &catalog(const std::string &catalogName) {
        for (auto &catalog : catalogs) {
            if (catalog.catalogName == catalogName) {
                return catalog;
            }
        }
        catalogs.emplace_back(CatalogDef());
        auto &catalog = catalogs.back();
        catalog.catalogName = catalogName;
        return catalog;
    }

public:
    std::vector<CatalogDef> catalogs;
};

} // namespace iquan
