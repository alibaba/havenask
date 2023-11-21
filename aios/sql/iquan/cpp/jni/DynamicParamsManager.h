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
#include <unordered_map>

#include "iquan/jni/LayerTableMeta.h"
#include "iquan/jni/LayerTableNorm.h"

namespace iquan {

class DynamicParamsManager {
public:
    bool addLayerTableMeta(const std::string &fullPath, LayerTableMetaPtr meta) {
        return layerTableMetaMap.insert(std::make_pair(fullPath, meta)).second;
    }

    const std::unordered_map<std::string, LayerTableMetaPtr> &getLayerTableMetaMap() const {
        return layerTableMetaMap;
    }

    bool normLayerTable(const std::vector<LayerTablePlanMeta> &layerTableMetas,
                        const std::vector<autil::legacy::Any> &params,
                        std::string &hashStr) {
        dynamicParamCache::LayerTableNorm layerTableNorm(layerTableMetas);
        return layerTableNorm.normalize(layerTableMetaMap, params, hashStr);
    }

private:
    std::unordered_map<std::string, LayerTableMetaPtr> layerTableMetaMap;
};

} // namespace iquan
