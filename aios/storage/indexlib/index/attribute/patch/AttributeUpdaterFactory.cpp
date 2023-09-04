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
#include "indexlib/index/attribute/patch/AttributeUpdaterFactory.h"

#include "indexlib/config/IIndexConfig.h"
#include "indexlib/index/attribute/AttributeFactory.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/attribute/patch/AttributeUpdater.h"
#include "indexlib/index/attribute/patch/AttributeUpdaterCreator.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, AttributeUpdaterFactory);

std::unique_ptr<AttributeUpdater>
AttributeUpdaterFactory::CreateAttributeUpdater(segmentid_t segId,
                                                const std::shared_ptr<config::IIndexConfig>& indexConfig)
{
    auto creator =
        AttributeFactory<AttributeUpdater, AttributeUpdaterCreator>::GetInstance()->GetAttributeInstanceCreator(
            indexConfig);
    assert(nullptr != creator);
    return creator->Create(segId, indexConfig);
}

bool AttributeUpdaterFactory::IsAttributeUpdatable(const std::shared_ptr<AttributeConfig>& attrConfig)
{
    auto factory = AttributeFactory<AttributeUpdater, AttributeUpdaterCreator>::GetInstance();
    {
        auto creator = factory->GetAttributeInstanceCreator(attrConfig);
        if (nullptr == creator) {
            return false;
        }
    }
    if (!attrConfig->IsMultiValue()) {
        return true;
    }
    return attrConfig->IsAttributeUpdatable();
}

} // namespace indexlibv2::index
