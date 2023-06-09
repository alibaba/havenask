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
#include "indexlib/index/common/field_format/spatial/SpatialFieldEncoder.h"

#include "indexlib/index/inverted_index/config/SpatialIndexConfig.h"

namespace indexlib::index {

AUTIL_LOG_SETUP(indexlib.index, SpatialFieldEncoder);

SpatialFieldEncoder::SpatialFieldEncoder(
    const std::vector<std::shared_ptr<indexlibv2::config::IIndexConfig>>& indexConfigs)
{
    Init<indexlibv2::config::SpatialIndexConfig>(indexConfigs);
}

SpatialFieldEncoder::~SpatialFieldEncoder() {}

int8_t SpatialFieldEncoder::TEST_GetTopLevel(fieldid_t fieldId) const { return std::get<1>(_infos[fieldId]); }
int8_t SpatialFieldEncoder::TEST_GetDetailLevel(fieldid_t fieldId) const { return std::get<2>(_infos[fieldId]); }
double SpatialFieldEncoder::TEST_GetDistanceLoss(fieldid_t fieldId) const { return std::get<3>(_infos[fieldId]); }

} // namespace indexlib::index
