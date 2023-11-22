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
#include "indexlib/index/attribute/AttributeDiskIndexer.h"

namespace indexlibv2::framework {
class Segment;
}
namespace indexlibv2::index {

#define DECLARE_ATTRIBUTE_DISK_INDEXER_CREATOR(classname, indextype)                                                   \
    class classname##Creator : public AttributeDiskIndexerCreator                                                      \
    {                                                                                                                  \
    public:                                                                                                            \
        FieldType GetAttributeType() const override { return indextype; }                                              \
        std::unique_ptr<AttributeDiskIndexer> Create(std::shared_ptr<AttributeMetrics> attributeMetrics,               \
                                                     const DiskIndexerParameter& indexerParam) const override          \
        {                                                                                                              \
            return std::make_unique<classname>(attributeMetrics, indexerParam);                                        \
        }                                                                                                              \
    };

class AttributeDiskIndexerCreator
{
public:
    AttributeDiskIndexerCreator() = default;
    virtual ~AttributeDiskIndexerCreator() = default;

public:
    virtual FieldType GetAttributeType() const = 0;
    virtual std::unique_ptr<AttributeDiskIndexer> Create(std::shared_ptr<AttributeMetrics> attributeMetrics,
                                                         const DiskIndexerParameter& indexerParam) const = 0;
};

class AttributeDefaultDiskIndexerFactory
{
public:
    static std::pair<Status, std::shared_ptr<IDiskIndexer>>
    CreateDefaultDiskIndexer(const std::shared_ptr<framework::Segment>& segment,
                             const std::shared_ptr<config::IIndexConfig>& config);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
