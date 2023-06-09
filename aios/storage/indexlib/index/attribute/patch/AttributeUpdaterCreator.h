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

#include "autil/Log.h"
#include "autil/NoCopyable.h"

namespace indexlibv2::config {
class IIndexConfig;
}

namespace indexlibv2::index {
class AttributeUpdater;

#define DECLARE_ATTRIBUTE_UPDATER_CREATOR(classname, indextype)                                                        \
    class classname##Creator : public AttributeUpdaterCreator                                                          \
    {                                                                                                                  \
    public:                                                                                                            \
        FieldType GetAttributeType() const override { return indextype; }                                              \
        std::unique_ptr<AttributeUpdater>                                                                              \
        Create(segmentid_t segId, const std::shared_ptr<config::IIndexConfig>& attrConfig) const override              \
        {                                                                                                              \
            return std::make_unique<classname>(segId, attrConfig);                                                     \
        }                                                                                                              \
    };

class AttributeUpdaterCreator : private autil::NoCopyable
{
public:
    AttributeUpdaterCreator() = default;
    virtual ~AttributeUpdaterCreator() = default;

public:
    virtual FieldType GetAttributeType() const = 0;
    virtual std::unique_ptr<AttributeUpdater> Create(segmentid_t segId,
                                                     const std::shared_ptr<config::IIndexConfig>& attrConfig) const = 0;
};

} // namespace indexlibv2::index
