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
#include "indexlib/index/statistics_term/StatisticsTermDiskIndexer.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/index/statistics_term/StatisticsTermLeafReader.h"

namespace indexlibv2::index {

Status StatisticsTermDiskIndexer::Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                       const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory)
{
    auto statIndexConfig = std::dynamic_pointer_cast<StatisticsTermIndexConfig>(indexConfig);
    if (!statIndexConfig) {
        return Status::InternalError("[%s:%s] is not an statistics term index", indexConfig->GetIndexType().c_str(),
                                     indexConfig->GetIndexName().c_str());
    }

    auto reader = std::make_shared<StatisticsTermLeafReader>();
    auto s = reader->Open(statIndexConfig, indexDirectory);
    if (s.IsOK()) {
        _reader = std::move(reader);
    }
    return s;
}

size_t
StatisticsTermDiskIndexer::EstimateMemUsed(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                           const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory)
{
    return 0;
}

size_t StatisticsTermDiskIndexer::EvaluateCurrentMemUsed() { return 0; }

} // namespace indexlibv2::index
