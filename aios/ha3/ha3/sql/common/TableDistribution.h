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
#include <algorithm>
#include <map>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/legacy/jsonizable.h" // IWYU pragma: export
#include "build_service/config/HashMode.h"

namespace isearch {
namespace sql {

typedef build_service::config::HashMode HashMode;

class TableDistribution : public autil::legacy::Jsonizable {
public:
    TableDistribution() {
        partCnt = 0;
    }

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("type", distributionType, distributionType);
        json.Jsonize("hash_mode", hashMode, hashMode);
        json.Jsonize("partition_cnt", partCnt, partCnt);
        json.Jsonize("hash_values", hashValues, hashValues);
        json.Jsonize("hash_values_sep", hashValuesSep, hashValuesSep);
        json.Jsonize("assigned_hash_values", debugHashValues, debugHashValues);
        json.Jsonize("assigned_partition_ids", debugPartIds, debugPartIds);
    }

public:
    std::string distributionType;
    std::string debugHashValues;
    std::string debugPartIds;
    HashMode hashMode;
    int32_t partCnt;
    std::map<std::string, std::vector<std::string>> hashValues;
    std::map<std::string, std::string> hashValuesSep;
};

} // namespace sql
} // namespace isearch
