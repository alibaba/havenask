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
#include "indexlib/index/operation_log/OperationLogIndexReader.h"

#include "indexlib/config/IIndexConfig.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/operation_log/OperationLogConfig.h"
#include "indexlib/index/operation_log/OperationLogDiskIndexer.h"
#include "indexlib/index/operation_log/OperationLogMemIndexer.h"
#include "indexlib/index/operation_log/OperationLogReplayer.h"

namespace indexlib::index {
namespace {
using indexlibv2::config::IIndexConfig;
} // namespace

AUTIL_LOG_SETUP(indexlib.index, OperationLogIndexReader);

OperationLogIndexReader::OperationLogIndexReader() {}

OperationLogIndexReader::~OperationLogIndexReader() {}

Status OperationLogIndexReader::Open(const std::shared_ptr<IIndexConfig>& indexConfig,
                                     const indexlibv2::framework::TabletData* tabletData)
{
    _indexConfig = std::dynamic_pointer_cast<OperationLogConfig>(indexConfig);
    if (!_indexConfig) {
        AUTIL_LOG(ERROR, "cast to operation log factory failed");
        return Status::Corruption("cast to operation log factory failed");
    }
    auto slice = tabletData->CreateSlice();
    for (auto iter = slice.begin(); iter != slice.end(); iter++) {
        auto segmentStatus = (*iter)->GetSegmentStatus();
        auto indexerPair = (*iter)->GetIndexer(indexConfig->GetIndexType(), indexConfig->GetIndexName());
        if (!indexerPair.first.IsOK()) {
            if (indexerPair.first.IsNotFound()) {
                continue;
            }
            AUTIL_LOG(ERROR, "get indexer type [%s] name [%s] from segment [%d] failed",
                      indexConfig->GetIndexType().c_str(), indexConfig->GetIndexName().c_str(),
                      (*iter)->GetSegmentId());
            return Status::Corruption("get operation log indexer failed");
        }
        auto indexer = indexerPair.second;
        std::shared_ptr<OperationLogIndexer> indexerBase;
        if (segmentStatus == indexlibv2::framework::Segment::SegmentStatus::ST_BUILT) {
            auto typedIndexer = std::dynamic_pointer_cast<OperationLogDiskIndexer>(indexer);
            indexerBase = std::dynamic_pointer_cast<OperationLogIndexer>(typedIndexer);

        } else {
            auto typedIndexer = std::dynamic_pointer_cast<OperationLogMemIndexer>(indexer);
            indexerBase = std::dynamic_pointer_cast<OperationLogIndexer>(typedIndexer);
        }
        if (!indexerBase) {
            AUTIL_LOG(ERROR, "cast to operation log mem indexer failed");
            return Status::Corruption("cast to operation log mem indexer failed");
        }
        _indexers.push_back(indexerBase);
    }
    return Status::OK();
}
std::unique_ptr<OperationLogReplayer> OperationLogIndexReader::CreateReplayer()
{
    return std::make_unique<OperationLogReplayer>(_indexers, _indexConfig);
}

} // namespace indexlib::index
