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
#include "indexlib/config/meta_re_truncate_config.h"

namespace indexlib { namespace config {

void MetaReTruncateConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("limit", _limit, _limit);
    json.Jsonize("diversity_constrain", _diversityConstrain, _diversityConstrain);
}

void MetaReTruncateConfig::Check() const
{
    if (!NeedReTruncate()) {
        return;
    }
    if (_diversityConstrain.NeedDistinct()) {
        bool valid = _diversityConstrain.GetDistCount() <= _limit;
        IE_CONFIG_ASSERT_PARAMETER_VALID(valid, "re_truncate: mDistCount should be less than mLimit");
        valid = _diversityConstrain.GetDistExpandLimit() >= _limit;
        IE_CONFIG_ASSERT_PARAMETER_VALID(valid, "re_truncate: mDistExpandLimit should be greater than mLimit");
    }
    if (_diversityConstrain.NeedFilter()) {
        if (!_diversityConstrain.IsFilterByMeta()) {
            IE_CONFIG_ASSERT_PARAMETER_VALID(false, "re_truncate: diversity constrain only support filter by meta");
        }
    }
    _diversityConstrain.Check();
}
bool MetaReTruncateConfig::operator==(const MetaReTruncateConfig& other) const
{
    return _limit == other._limit and _diversityConstrain == other._diversityConstrain;
}

}} // namespace indexlib::config
