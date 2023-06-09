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
#include "indexlib/index/statistics_term/StatisticsTermIndexFactory.h"

#include "autil/Log.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/IndexerParameter.h"
#include "indexlib/index/statistics_term/Constant.h"
#include "indexlib/index/statistics_term/StatisticsTermDiskIndexer.h"
#include "indexlib/index/statistics_term/StatisticsTermIndexConfig.h"
#include "indexlib/index/statistics_term/StatisticsTermLeafReader.h"
#include "indexlib/index/statistics_term/StatisticsTermMemIndexer.h"
#include "indexlib/index/statistics_term/StatisticsTermMerger.h"
#include "indexlib/index/statistics_term/StatisticsTermReader.h"

namespace indexlibv2::index {
AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.index, StatisticsTermIndexFactory);

std::shared_ptr<IDiskIndexer>
StatisticsTermIndexFactory::CreateDiskIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                              const IndexerParameter& indexerParam) const
{
    return std::make_shared<StatisticsTermDiskIndexer>();
}

std::shared_ptr<IMemIndexer>
StatisticsTermIndexFactory::CreateMemIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                             const IndexerParameter& indexerParam) const
{
    std::map<std::string, size_t> initialKeyCount;
    if (indexerParam.lastSegmentMetrics != nullptr) {
        auto statConfig = std::dynamic_pointer_cast<StatisticsTermIndexConfig>(indexConfig);
        assert(statConfig != nullptr);
        for (const auto& indexName : statConfig->GetInvertedIndexNames()) {
            auto key = StatisticsTermMemIndexer::GetStatisticsTermKey(statConfig->GetIndexName(), indexName);
            auto lastSegmentKeyCount = indexerParam.lastSegmentMetrics->GetDistinctTermCount(key);
            initialKeyCount[key] = lastSegmentKeyCount;
        }
    }
    return std::make_shared<StatisticsTermMemIndexer>(initialKeyCount);
}

std::unique_ptr<IIndexReader>
StatisticsTermIndexFactory::CreateIndexReader(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                              const IndexerParameter& indexerParam) const
{
    return std::make_unique<StatisticsTermReader>();
}

std::unique_ptr<IIndexMerger>
StatisticsTermIndexFactory::CreateIndexMerger(const std::shared_ptr<config::IIndexConfig>& indexConfig) const
{
    return std::make_unique<StatisticsTermMerger>();
}

std::unique_ptr<config::IIndexConfig> StatisticsTermIndexFactory::CreateIndexConfig(const autil::legacy::Any& any) const
{
    return std::make_unique<StatisticsTermIndexConfig>();
}

std::string StatisticsTermIndexFactory::GetIndexPath() const { return STATISTICS_TERM_INDEX_PATH; }

REGISTER_INDEX(statistics_term, StatisticsTermIndexFactory);
} // namespace indexlibv2::index
