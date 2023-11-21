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
#include "indexlib/index/pack_attribute/PackAttributeIndexFactory.h"

#include "indexlib/framework/MetricsManager.h"
#include "indexlib/index/attribute/AttributeDiskIndexerCreator.h"
#include "indexlib/index/pack_attribute/Common.h"
#include "indexlib/index/pack_attribute/MultiSlicePackAttributeDiskIndexer.h"
#include "indexlib/index/pack_attribute/PackAttributeConfig.h"
#include "indexlib/index/pack_attribute/PackAttributeDiskIndexer.h"
#include "indexlib/index/pack_attribute/PackAttributeIndexMerger.h"
#include "indexlib/index/pack_attribute/PackAttributeMemIndexer.h"
#include "indexlib/index/pack_attribute/PackAttributeMetrics.h"
#include "indexlib/index/pack_attribute/PackAttributeReader.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, PackAttributeIndexFactory);

class PackAttributeDiskIndexerCreator : public AttributeDiskIndexerCreator
{
public:
    FieldType GetAttributeType() const override { return ft_string; }
    std::unique_ptr<AttributeDiskIndexer> Create(std::shared_ptr<AttributeMetrics> attributeMetrics,
                                                 const DiskIndexerParameter& indexerParam) const override
    {
        return std::make_unique<PackAttributeDiskIndexer>(attributeMetrics, indexerParam);
    }
};

static std::shared_ptr<PackAttributeMetrics>
CreatePackAttributeMetrics(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                           framework::MetricsManager* metricsManager)
{
    if (metricsManager != nullptr) {
        auto packAttributeMetrics = std::dynamic_pointer_cast<PackAttributeMetrics>(metricsManager->CreateMetrics(
            indexConfig->GetIndexName(), [&metricsManager]() -> std::shared_ptr<framework::IMetrics> {
                return std::make_shared<PackAttributeMetrics>(metricsManager);
            }));
        assert(nullptr != packAttributeMetrics);
        return packAttributeMetrics;
    }
    return nullptr;
}

PackAttributeIndexFactory::PackAttributeIndexFactory() {}
PackAttributeIndexFactory::~PackAttributeIndexFactory() {}

std::shared_ptr<IDiskIndexer>
PackAttributeIndexFactory::CreateDiskIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                             const DiskIndexerParameter& indexerParam) const
{
    static PackAttributeDiskIndexerCreator creator;
    auto packAttributeMetrics = CreatePackAttributeMetrics(indexConfig, indexerParam.metricsManager);
    return std::make_shared<MultiSlicePackAttributeDiskIndexer>(packAttributeMetrics, indexerParam, &creator);
}

std::shared_ptr<IMemIndexer>
PackAttributeIndexFactory::CreateMemIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                            const MemIndexerParameter& indexerParam) const
{
    return std::make_shared<PackAttributeMemIndexer>(indexerParam);
}

std::unique_ptr<IIndexReader>
PackAttributeIndexFactory::CreateIndexReader(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                             const IndexReaderParameter& indexReaderParam) const
{
    auto packAttributeMetrics = CreatePackAttributeMetrics(indexConfig, indexReaderParam.metricsManager);
    return std::make_unique<PackAttributeReader>(packAttributeMetrics);
}

std::unique_ptr<IIndexMerger>
PackAttributeIndexFactory::CreateIndexMerger(const std::shared_ptr<config::IIndexConfig>& indexConfig) const
{
    return std::make_unique<PackAttributeIndexMerger>();
}

std::unique_ptr<config::IIndexConfig> PackAttributeIndexFactory::CreateIndexConfig(const autil::legacy::Any& any) const
{
    return std::make_unique<PackAttributeConfig>();
}

std::string PackAttributeIndexFactory::GetIndexPath() const { return index::PACK_ATTRIBUTE_INDEX_PATH; }

REGISTER_INDEX_FACTORY(pack_attribute, PackAttributeIndexFactory);
} // namespace indexlibv2::index
