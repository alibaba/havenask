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
#include "indexlib/index/aggregation/AggregationIndexFactory.h"

#include "autil/Log.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/IndexerParameter.h"
#include "indexlib/index/aggregation/AggregationDiskIndexer.h"
#include "indexlib/index/aggregation/AggregationIndexConfig.h"
#include "indexlib/index/aggregation/AggregationIndexFieldsParser.h"
#include "indexlib/index/aggregation/AggregationIndexMerger.h"
#include "indexlib/index/aggregation/AggregationMemIndexer.h"
#include "indexlib/index/aggregation/AggregationReader.h"
#include "indexlib/index/aggregation/Constants.h"

namespace indexlibv2::index {
AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.index, AggregationIndexFactory);

std::shared_ptr<IDiskIndexer>
AggregationIndexFactory::CreateDiskIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                           const IndexerParameter& indexerParam) const
{
    return std::make_shared<AggregationDiskIndexer>();
}

std::shared_ptr<IMemIndexer>
AggregationIndexFactory::CreateMemIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                          const IndexerParameter& indexerParam) const
{
    size_t lastSegmentKeyCount = 0;
    if (indexerParam.lastSegmentMetrics != nullptr) {
        lastSegmentKeyCount = indexerParam.lastSegmentMetrics->GetDistinctTermCount(indexConfig->GetIndexName());
    }
    return std::make_shared<AggregationMemIndexer>(lastSegmentKeyCount);
}

std::unique_ptr<IIndexReader>
AggregationIndexFactory::CreateIndexReader(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                           const IndexerParameter& indexerParam) const
{
    return std::make_unique<AggregationReader>();
}

std::unique_ptr<IIndexMerger>
AggregationIndexFactory::CreateIndexMerger(const std::shared_ptr<config::IIndexConfig>& indexConfig) const
{
    return std::make_unique<AggregationIndexMerger>();
}

std::unique_ptr<config::IIndexConfig> AggregationIndexFactory::CreateIndexConfig(const autil::legacy::Any& any) const
{
    return std::make_unique<config::AggregationIndexConfig>();
}

std::string AggregationIndexFactory::GetIndexPath() const { return AGGREGATION_INDEX_PATH; }

std::unique_ptr<document::IIndexFieldsParser> AggregationIndexFactory::CreateIndexFieldsParser()
{
    return std::make_unique<AggregationIndexFieldsParser>();
}

REGISTER_INDEX(aggregation, AggregationIndexFactory);
} // namespace indexlibv2::index
