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

#include "indexlib/base/Types.h"
#include "indexlib/index/MemIndexerParameter.h"
#include "indexlib/index/attribute/AttributeMemIndexer.h"

namespace indexlib2::config {
class IIndexConfig;
}

namespace indexlibv2::index {
#define DECLARE_ATTRIBUTE_MEM_INDEXER_CREATOR(classname, indextype)                                                    \
    class classname##Creator : public AttributeMemIndexerCreator                                                       \
    {                                                                                                                  \
    public:                                                                                                            \
        FieldType GetAttributeType() const override { return indextype; }                                              \
        std::unique_ptr<AttributeMemIndexer> Create(const std::shared_ptr<config::IIndexConfig>& indexConfig,          \
                                                    const MemIndexerParameter& indexerParam) const override            \
        {                                                                                                              \
            return std::make_unique<classname>(indexerParam);                                                          \
        }                                                                                                              \
    };

class AttributeMemIndexerCreator
{
public:
    AttributeMemIndexerCreator() = default;
    virtual ~AttributeMemIndexerCreator() = default;

    virtual FieldType GetAttributeType() const = 0;
    virtual std::unique_ptr<AttributeMemIndexer> Create(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                        const MemIndexerParameter& indexerParam) const = 0;
};

} // namespace indexlibv2::index
