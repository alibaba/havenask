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
#include "iquan/common/catalog/LayerTableModel.h"

namespace iquan {
class Layer;

class LayerTableMeta {
public:
    LayerTableMeta() {}
    const std::unordered_map<std::string, std::unordered_set<std::string>> &getStrValueMap() {
        return _strValueMap;
    };
    const std::unordered_map<std::string, std::vector<int64_t>> &getIntValueMap() {
        return _intValueMap;
    };
    bool init(const LayerTableModel &layerTable);

private:
    bool isInt64(autil::legacy::Any jsonAny, int64_t &value);
    bool processStringType(const LayerTableDef &layerTable,
                           const std::string &layerTableName,
                           const std::string &fieldName);
    bool processIntType(const LayerTableDef &layerTable,
                        const std::string &layerTableName,
                        const std::string &fieldName,
                        const std::string &method);
    std::string layerInfoTypeErrMsg(const std::string &layerTableName, const Layer &layer) const;
    std::string layerInfoRangeErrMsg(const std::string &layerTableName, const Layer &layer) const;
    std::string layerInfoFieldErrMsg(const std::string &layerTableName,
                                     const Layer &layer,
                                     const std::string &fieldName) const;

private:
    std::unordered_map<std::string, std::unordered_set<std::string>> _strValueMap;
    std::unordered_map<std::string, std::vector<int64_t>> _intValueMap;

    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<LayerTableMeta> LayerTableMetaPtr;

} // namespace iquan
