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
#include "indexlib/index/inverted_index/section_attribute/SectionAttributeIndexFactory.h"

#include "indexlib/framework/MetricsManager.h"
#include "indexlib/index/attribute/MultiValueAttributeDiskIndexer.h"
#include "indexlib/index/inverted_index/section_attribute/SectionAttributeMerger.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, SectionAttributeIndexFactory);

SectionAttributeIndexFactory::SectionAttributeIndexFactory() {}
SectionAttributeIndexFactory::~SectionAttributeIndexFactory() {}

std::shared_ptr<indexlibv2::index::IDiskIndexer>
SectionAttributeIndexFactory::CreateDiskIndexer(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                                                const indexlibv2::index::IndexerParameter& indexerParam) const
{
    std::shared_ptr<indexlibv2::index::AttributeMetrics> attributeMetrics;
    if (indexerParam.metricsManager != nullptr) {
        attributeMetrics =
            std::dynamic_pointer_cast<indexlibv2::index::AttributeMetrics>(indexerParam.metricsManager->CreateMetrics(
                indexConfig->GetIndexName(), [&indexerParam]() -> std::shared_ptr<indexlibv2::framework::IMetrics> {
                    return std::make_shared<indexlibv2::index::AttributeMetrics>(
                        indexerParam.metricsManager->GetMetricsReporter());
                }));
        assert(nullptr != attributeMetrics);
    }
    return std::make_shared<indexlibv2::index::MultiValueAttributeDiskIndexer<char>>(attributeMetrics, indexerParam);
}

std::unique_ptr<indexlibv2::index::IIndexMerger> SectionAttributeIndexFactory::CreateIndexMerger(
    const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig) const
{
    return std::make_unique<SectionAttributeMerger>();
}

} // namespace indexlib::index
