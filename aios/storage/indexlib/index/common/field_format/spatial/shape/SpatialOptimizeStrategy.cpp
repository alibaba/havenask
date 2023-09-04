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
#include "indexlib/index/common/field_format/spatial/shape/SpatialOptimizeStrategy.h"

#include <unistd.h>

#include "autil/EnvUtil.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, SpatialOptimizeStrategy);

SpatialOptimizeStrategy::SpatialOptimizeStrategy() : _enableOptimize(true)
{
    std::string envParam = autil::EnvUtil::getEnv("DISABLE_SPATIAL_OPTIMIZE");
    if (envParam == "true") {
        AUTIL_LOG(INFO, "disable spatial index optimize!");
        _enableOptimize = false;
    }
}

} // namespace indexlib::index
