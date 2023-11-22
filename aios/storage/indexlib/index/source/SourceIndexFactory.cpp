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
#include "indexlib/index/source/SourceIndexFactory.h"

#include "indexlib/index/source/Common.h"
#include "indexlib/index/source/SourceDiskIndexer.h"
#include "indexlib/index/source/SourceMemIndexer.h"
#include "indexlib/index/source/SourceMerger.h"
#include "indexlib/index/source/SourceReader.h"
#include "indexlib/index/source/config/SourceIndexConfig.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, SourceIndexFactory);

SourceIndexFactory::SourceIndexFactory() {}

SourceIndexFactory::~SourceIndexFactory() {}

std::shared_ptr<IDiskIndexer>
SourceIndexFactory::CreateDiskIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                      const DiskIndexerParameter& indexerParam) const
{
    return std::make_shared<SourceDiskIndexer>(indexerParam);
}
std::shared_ptr<IMemIndexer>
SourceIndexFactory::CreateMemIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                     const MemIndexerParameter& indexerParam) const
{
    return std::make_shared<SourceMemIndexer>(indexerParam);
}
std::unique_ptr<IIndexReader>
SourceIndexFactory::CreateIndexReader(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                      const IndexReaderParameter&) const
{
    return std::make_unique<SourceReader>();
}
std::unique_ptr<IIndexMerger>
SourceIndexFactory::CreateIndexMerger(const std::shared_ptr<config::IIndexConfig>& indexConfig) const
{
    return std::make_unique<SourceMerger>();
}
std::unique_ptr<config::IIndexConfig> SourceIndexFactory::CreateIndexConfig(const autil::legacy::Any& any) const
{
    return std::make_unique<config::SourceIndexConfig>();
}
std::string SourceIndexFactory::GetIndexPath() const { return SOURCE_INDEX_PATH; }
REGISTER_INDEX_FACTORY(source, SourceIndexFactory);
} // namespace indexlibv2::index
