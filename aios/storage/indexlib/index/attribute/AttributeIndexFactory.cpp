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
#include "indexlib/index/attribute/AttributeIndexFactory.h"

#include "indexlib/framework/MetricsManager.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/attribute/AttributeDiskIndexerCreator.h"
#include "indexlib/index/attribute/AttributeFactory.h"
#include "indexlib/index/attribute/AttributeMemIndexerCreator.h"
#include "indexlib/index/attribute/AttributeMetrics.h"
#include "indexlib/index/attribute/AttributeReaderCreator.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/attribute/merger/AttributeMergerCreator.h"

using namespace std;

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, AttributeIndexFactory);

AttributeIndexFactory::AttributeIndexFactory() {}

AttributeIndexFactory::~AttributeIndexFactory() {}

std::shared_ptr<IDiskIndexer>
AttributeIndexFactory::CreateDiskIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                         const IndexerParameter& indexerParam) const
{
    AttributeDiskIndexerCreator* creator =
        AttributeFactory<AttributeDiskIndexer, AttributeDiskIndexerCreator>::GetInstance()->GetAttributeInstanceCreator(
            indexConfig);
    assert(nullptr != creator);
    std::shared_ptr<AttributeMetrics> attributeMetrics;
    if (indexerParam.metricsManager != nullptr) {
        attributeMetrics = std::dynamic_pointer_cast<AttributeMetrics>(indexerParam.metricsManager->CreateMetrics(
            indexConfig->GetIndexName(), [&indexerParam]() -> std::shared_ptr<framework::IMetrics> {
                return std::make_shared<AttributeMetrics>(indexerParam.metricsManager->GetMetricsReporter());
            }));
        assert(nullptr != attributeMetrics);
    }
    return std::make_shared<MultiSliceAttributeDiskIndexer>(attributeMetrics, indexerParam, creator);
}

std::shared_ptr<IMemIndexer>
AttributeIndexFactory::CreateMemIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                        const IndexerParameter& indexerParam) const
{
    AttributeMemIndexerCreator* creator =
        AttributeFactory<AttributeMemIndexer, AttributeMemIndexerCreator>::GetInstance()->GetAttributeInstanceCreator(
            indexConfig);
    assert(nullptr != creator);
    return creator->Create(indexerParam);
}
std::unique_ptr<IIndexReader>
AttributeIndexFactory::CreateIndexReader(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                         const IndexerParameter& indexerParam) const
{
    AttributeReaderCreator* creator =
        AttributeFactory<AttributeReader, AttributeReaderCreator>::GetInstance()->GetAttributeInstanceCreator(
            indexConfig);
    assert(nullptr != creator);
    return creator->Create(indexConfig, indexerParam);
}
std::unique_ptr<IIndexMerger>
AttributeIndexFactory::CreateIndexMerger(const std::shared_ptr<config::IIndexConfig>& indexConfig) const
{
    AttributeMergerCreator* creator =
        AttributeFactory<AttributeMerger, AttributeMergerCreator>::GetInstance()->GetAttributeInstanceCreator(
            indexConfig);
    assert(nullptr != creator);
    const auto& attrConfig = std::dynamic_pointer_cast<config::AttributeConfig>(indexConfig);
    if (nullptr == attrConfig) {
        AUTIL_LOG(ERROR, "create attribute index merger [%s] fail because of wrong config type",
                  indexConfig->GetIndexName().c_str());
        return nullptr;
    }
    bool isUniqEncoded = attrConfig->IsUniqEncode();
    return creator->Create(isUniqEncoded);
}
std::unique_ptr<config::IIndexConfig> AttributeIndexFactory::CreateIndexConfig(const autil::legacy::Any& any) const
{
    return std::make_unique<config::AttributeConfig>(config::AttributeConfig::ct_normal);
}
std::string AttributeIndexFactory::GetIndexPath() const { return index::ATTRIBUTE_INDEX_PATH; }

REGISTER_INDEX(attribute, AttributeIndexFactory);
} // namespace indexlibv2::index
