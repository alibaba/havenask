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

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_updater.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class TimestampAttributeUpdater : public SingleValueAttributeUpdater<uint64_t>
{
public:
    TimestampAttributeUpdater(util::BuildResourceMetrics* buildResourceMetrics, segmentid_t segId,
                              const config::AttributeConfigPtr& attrConfig)
        : SingleValueAttributeUpdater<uint64_t>(buildResourceMetrics, segId, attrConfig)
    {
    }
    ~TimestampAttributeUpdater() {}

public:
    class Creator : public AttributeUpdaterCreator
    {
    public:
        FieldType GetAttributeType() const { return ft_timestamp; }

        AttributeUpdater* Create(util::BuildResourceMetrics* buildResourceMetrics, segmentid_t segId,
                                 const config::AttributeConfigPtr& attrConfig) const
        {
            return new TimestampAttributeUpdater(buildResourceMetrics, segId, attrConfig);
        }
    };
};

DEFINE_SHARED_PTR(TimestampAttributeUpdater);

///////////////////////////////////////////////////////
}} // namespace indexlib::index
