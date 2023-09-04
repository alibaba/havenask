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

#include <unordered_map>

#include "autil/Log.h"
#include "iquan/common/catalog/LayerTablePlanMetaDef.h"
#include "iquan/jni/LayerTableMeta.h"

namespace iquan {
namespace dynamicParamCache {

class LayerTableNorm {
public:
    LayerTableNorm(const std::vector<LayerTablePlanMeta> &metas);
    bool normalize(const std::unordered_map<std::string, LayerTableMetaPtr> &layerTableMetaMap,
                   const std::vector<autil::legacy::Any> &params,
                   std::string &result) const;

private:
    bool doNorm(const std::unordered_map<std::string, LayerTableMetaPtr> &layerTableMetaMap,
                const LayerTablePlanMeta &planMeta,
                const std::vector<autil::legacy::Any> &params,
                std::string &res) const;

private:
    const std::vector<LayerTablePlanMeta> &planMetas;

    AUTIL_LOG_DECLARE();
};

} // namespace dynamicParamCache
} // namespace iquan
