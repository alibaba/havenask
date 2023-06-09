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

#include <map>
#include <memory>

#include "autil/Lock.h"
#include "indexlib/base/Types.h"
#include "indexlib/util/Singleton.h"

namespace indexlibv2::config {
class AttributeConfig;
}

namespace indexlibv2::config {
class IIndexConfig;
}
namespace indexlibv2::index {
class AttributeUpdater;
class AttributeUpdaterFactory : public indexlib::util::Singleton<AttributeUpdaterFactory>
{
protected:
    AttributeUpdaterFactory() = default;
    friend class indexlib::util::LazyInstantiation;

public:
    ~AttributeUpdaterFactory() = default;

public:
    std::unique_ptr<AttributeUpdater> CreateAttributeUpdater(segmentid_t segId,
                                                             const std::shared_ptr<config::IIndexConfig>& indexConfig);

    bool IsAttributeUpdatable(const std::shared_ptr<config::AttributeConfig>& attrConfig);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
