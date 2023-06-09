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
#include "indexlib/index/orc/OrcIndexFactory.h"

#include "indexlib/index/orc/AlogOrcLoggerAdapter.h"
#include "indexlib/index/orc/OrcConfig.h"
#include "indexlib/index/orc/OrcDiskIndexer.h"
#include "indexlib/index/orc/OrcMemIndexer.h"
#include "indexlib/index/orc/OrcMerger.h"
#include "indexlib/index/orc/OrcReader.h"

using namespace std;

namespace indexlibv2::index {

// make sure init after alog
std::once_flag ORC_LOG_INIT_FLAG;
void InitOrcLog()
{
    std::call_once(ORC_LOG_INIT_FLAG, []() {
        orc::Logger::RegisterLogger(
            ORC_LOGGER_IMPL, [](const std::string& path) { return std::make_unique<AlogOrcLoggerAdapter>(path); });
    });
}

AUTIL_LOG_SETUP(indexlib.index, OrcIndexFactory);

OrcIndexFactory::OrcIndexFactory() {}

OrcIndexFactory::~OrcIndexFactory() {}

std::shared_ptr<IDiskIndexer>
OrcIndexFactory::CreateDiskIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                   const IndexerParameter& indexerParam) const
{
    InitOrcLog();
    return std::make_shared<index::OrcDiskIndexer>();
}

std::shared_ptr<IMemIndexer> OrcIndexFactory::CreateMemIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                               const IndexerParameter& indexerParam) const
{
    InitOrcLog();
    return std::make_shared<index::OrcMemIndexer>();
}

std::unique_ptr<IIndexReader>
OrcIndexFactory::CreateIndexReader(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                   const IndexerParameter& indexerParam) const
{
    InitOrcLog();
    return std::make_unique<OrcReader>();
}

std::unique_ptr<IIndexMerger>
OrcIndexFactory::CreateIndexMerger(const std::shared_ptr<config::IIndexConfig>& indexConfig) const
{
    InitOrcLog();
    return std::make_unique<index::OrcMerger>();
}

std::unique_ptr<config::IIndexConfig> OrcIndexFactory::CreateIndexConfig(const autil::legacy::Any& any) const
{
    InitOrcLog();
    return std::make_unique<config::OrcConfig>();
}

std::string OrcIndexFactory::GetIndexPath() const { return "orc"; }
REGISTER_INDEX(orc, OrcIndexFactory);
} // namespace indexlibv2::index
