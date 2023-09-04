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

#include <any>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "autil/Log.h"
#include "iquan/common/catalog/LayerTableDef.h"

namespace iquan {

class LayerTableMeta {
public:
    LayerTableMeta(const LayerTableDef &layerTable);
    const std::unordered_map<std::string, std::unordered_set<std::string>> &getStrValueMap() {
        return strValueMap;
    };
    const std::unordered_map<std::string, std::vector<int64_t>> &getIntValueMap() {
        return intValueMap;
    };

private:
    std::unordered_map<std::string, std::unordered_set<std::string>> strValueMap;
    std::unordered_map<std::string, std::vector<int64_t>> intValueMap;

    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<LayerTableMeta> LayerTableMetaPtr;

} // namespace iquan
