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
#include "build_service/proto/DataDescription.h"

#include <cstddef>
#include <ext/alloc_traits.h>
#include <map>
#include <utility>

#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/config/CLIOptionNames.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

namespace build_service { namespace proto {
BS_LOG_SETUP(proto, DataDescription);

DataDescription::DataDescription() {}

DataDescription::~DataDescription() {}

bool DataDescription::operator==(const DataDescription& other) const
{
    if (kvMap != other.kvMap) {
        return false;
    }

    if (parserConfigs.size() != other.parserConfigs.size()) {
        return false;
    }

    for (size_t i = 0; i < parserConfigs.size(); ++i) {
        if (parserConfigs[i] != other.parserConfigs[i]) {
            return false;
        }
    }
    return true;
}

void DataDescription::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON) {
        for (auto& kv : kvMap) {
            json.Jsonize(kv.first, kv.second);
        }
        if (!parserConfigs.empty()) {
            json.Jsonize(config::DATA_DESCRIPTION_PARSER_CONFIG, parserConfigs);
        }
    } else {
        kvMap.clear();
        parserConfigs.clear();
        json.Jsonize(config::DATA_DESCRIPTION_PARSER_CONFIG, parserConfigs, parserConfigs);
        auto jsonMap = json.GetMap();
        for (const auto& kv : jsonMap) {
            if (kv.first != config::DATA_DESCRIPTION_PARSER_CONFIG) {
                KeyValueMap::mapped_type v;
                FromJson(v, kv.second);
                kvMap.insert(make_pair(kv.first, v));
            }
        }
    }
}
}} // namespace build_service::proto
