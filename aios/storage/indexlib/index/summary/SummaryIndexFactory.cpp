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
#include "indexlib/index/summary/SummaryIndexFactory.h"

#include "indexlib/index/IndexReaderParameter.h"
#include "indexlib/index/MemIndexerParameter.h"
#include "indexlib/index/summary/Common.h"
#include "indexlib/index/summary/SummaryDiskIndexer.h"
#include "indexlib/index/summary/SummaryMemIndexer.h"
#include "indexlib/index/summary/SummaryMerger.h"
#include "indexlib/index/summary/SummaryReader.h"
#include "indexlib/index/summary/config/SummaryIndexConfig.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, SummaryIndexFactory);

std::shared_ptr<IDiskIndexer>
SummaryIndexFactory::CreateDiskIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                       const DiskIndexerParameter& indexerParam) const
{
    return std::make_shared<SummaryDiskIndexer>(indexerParam);
}

std::shared_ptr<IMemIndexer>
SummaryIndexFactory::CreateMemIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                      const MemIndexerParameter&) const
{
    return std::make_shared<SummaryMemIndexer>();
}

std::unique_ptr<IIndexReader>
SummaryIndexFactory::CreateIndexReader(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                       const IndexReaderParameter&) const
{
    return std::make_unique<SummaryReader>();
}

std::unique_ptr<IIndexMerger>
SummaryIndexFactory::CreateIndexMerger(const std::shared_ptr<config::IIndexConfig>& indexConfig) const
{
    return std::make_unique<SummaryMerger>();
}

std::unique_ptr<config::IIndexConfig> SummaryIndexFactory::CreateIndexConfig(const autil::legacy::Any& any) const
{
    return std::make_unique<config::SummaryIndexConfig>();
}
std::string SummaryIndexFactory::GetIndexPath() const { return index::SUMMARY_INDEX_PATH; }

REGISTER_INDEX_FACTORY(summary, SummaryIndexFactory);
} // namespace indexlibv2::index
