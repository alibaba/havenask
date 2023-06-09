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

#include "autil/Log.h"
#include "indexlib/base/Types.h"

namespace indexlibv2::config {
class KVIndexConfig;
}

namespace indexlibv2::framework {
class TabletData;
}

namespace indexlibv2::index {

class AdapterIgnoreFieldCalculator
{
public:
    AdapterIgnoreFieldCalculator();
    ~AdapterIgnoreFieldCalculator();

public:
    bool Init(const std::shared_ptr<indexlibv2::config::KVIndexConfig>& kvIndexConfig,
              const framework::TabletData* tabletData);

    /*  example:
        schema0:  field1, field2
        schema1: delete field1
        schema2: add field1, field3
        * field1 should be ignored when adaptor read from schema0 format segment
        * cause field1 is deleted before
     */
    // replaced fields (drop & add back) should be ignored
    std::vector<std::string> GetIgnoreFields(schemaid_t beginSchemaId, schemaid_t endSchemaId);

private:
    std::map<schemaid_t, std::vector<std::string>> _removeFieldRoadMap;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
