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

#include <map>
#include <string>
#include <vector>

#include "autil/legacy/jsonizable.h"
#include "build_service/config/HashMode.h"

namespace suez {

class SortDescription : public autil::legacy::Jsonizable {
public:
    SortDescription(const std::string &field_ = "", const std::string &order_ = "") : field(field_), order(order_) {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("sort_field", field);
        json.Jsonize("sort_pattern", order, order);
    }

public:
    std::string field;
    std::string order;
};

class SortDescriptions : public autil::legacy::Jsonizable {
public:
    SortDescriptions() : sortBuild(false) {}
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        if (json.GetMode() == FROM_JSON) {
            json.Jsonize("sort_build", sortBuild, sortBuild);
            if (sortBuild) {
                json.Jsonize("sort_descriptions", sortDescs, sortDescs);
            }
        }
    }

public:
    bool sortBuild = false;
    std::vector<SortDescription> sortDescs;
};

class TableDistribution : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("hash_mode", hashMode, hashMode);
    }

public:
    int32_t partitionCnt = 1;
    build_service::config::HashMode hashMode;
};

class TableDefConfig : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("cluster_config", tableDist, tableDist);
        json.Jsonize("build_option_config", sortDescriptions, sortDescriptions);
    }

public:
    TableDistribution tableDist;
    SortDescriptions sortDescriptions;
};

} // namespace suez
