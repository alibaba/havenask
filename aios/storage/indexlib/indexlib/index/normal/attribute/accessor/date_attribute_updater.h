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

class DateAttributeUpdater : public SingleValueAttributeUpdater<uint32_t>
{
public:
    DateAttributeUpdater(util::BuildResourceMetrics* buildResourceMetrics, segmentid_t segId,
                         const config::AttributeConfigPtr& attrConfig)
        : SingleValueAttributeUpdater<uint32_t>(buildResourceMetrics, segId, attrConfig)
    {
    }
    ~DateAttributeUpdater() {}

public:
    class Creator : public AttributeUpdaterCreator
    {
    public:
        FieldType GetAttributeType() const { return ft_date; }

        AttributeUpdater* Create(util::BuildResourceMetrics* buildResourceMetrics, segmentid_t segId,
                                 const config::AttributeConfigPtr& attrConfig) const
        {
            return new DateAttributeUpdater(buildResourceMetrics, segId, attrConfig);
        }
    };
};

DEFINE_SHARED_PTR(DateAttributeUpdater);

///////////////////////////////////////////////////////
}} // namespace indexlib::index
