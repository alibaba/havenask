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
#include "indexlib/index/aggregation/AggregationDiskIndexer.h"

#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/aggregation/AggregationLeafReader.h"

namespace indexlibv2::index {

Status AggregationDiskIndexer::Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                    const std::shared_ptr<indexlib::file_system::IDirectory>& indexRoot)
{
    auto aggIndexConfig = std::dynamic_pointer_cast<config::AggregationIndexConfig>(indexConfig);
    if (!aggIndexConfig) {
        return Status::InternalError("[%s:%s] is not an aggregation index", indexConfig->GetIndexType().c_str(),
                                     indexConfig->GetIndexName().c_str());
    }

    auto reader = std::make_shared<AggregationLeafReader>();
    auto s = reader->Open(aggIndexConfig, indexlib::file_system::IDirectory::ToLegacyDirectory(indexRoot));
    if (s.IsOK()) {
        _reader = std::move(reader);
    }
    if (!s.IsOK()) {
        return s;
    }

    if (aggIndexConfig->SupportDelete()) {
        auto deleteReader = std::make_shared<AggregationLeafReader>();
        auto [s, indexDir] = indexRoot->GetDirectory(indexConfig->GetIndexName()).StatusWith();
        if (!s.IsOK()) {
            return s;
        }
        s = deleteReader->Open(aggIndexConfig->GetDeleteConfig(),
                               indexlib::file_system::IDirectory::ToLegacyDirectory(indexDir));
        if (s.IsOK()) {
            _deleteReader = std::move(deleteReader);
        }
    }
    return s;
}

size_t AggregationDiskIndexer::EstimateMemUsed(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                               const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory)
{
    return 0;
}

size_t AggregationDiskIndexer::EvaluateCurrentMemUsed() { return 0; }

} // namespace indexlibv2::index
