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
#include "indexlib/index/statistics_term/StatisticsTermReader.h"

#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "autil/result/Errors.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/statistics_term/StatisticsTermDiskIndexer.h"

namespace indexlibv2::index {
AUTIL_DECLARE_AND_SETUP_LOGGER(index, StatisticsTermReader);

Status StatisticsTermReader::Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                  const framework::TabletData* tabletData)
{
    _indexConfig = std::dynamic_pointer_cast<StatisticsTermIndexConfig>(indexConfig);
    if (_indexConfig == nullptr) {
        return Status::InternalError("expect StatisticsTermIndexConfig");
    }
    // handle built segment only
    for (auto segment : tabletData->CreateSlice(framework::Segment::SegmentStatus::ST_BUILT)) {
        auto [s, indexer] = segment->GetIndexer(_indexConfig->GetIndexType(), _indexConfig->GetIndexName());
        if (!s.IsOK()) {
            AUTIL_LOG(ERROR, "get indexer[%s] from segment[%d] failed.", _indexConfig->GetIndexName().c_str(),
                      segment->GetSegmentId());
            return s;
        }
        auto diskIndexer = std::dynamic_pointer_cast<StatisticsTermDiskIndexer>(indexer);
        if (diskIndexer == nullptr) {
            return Status::InternalError("expect StatisticsTermDiskIndexer, actual: %s", typeid(indexer).name());
        }

        auto reader = diskIndexer->GetReader();
        if (reader == nullptr) {
            AUTIL_LOG(ERROR, "get reader from indexer failed, segment[%d]", segment->GetSegmentId());
            return Status::InternalError("create segment reader for segment %d failed", segment->GetSegmentId());
        }
        _segmentReaders.emplace_back(std::move(reader));
    }
    return Status::OK();
}

const std::vector<std::shared_ptr<StatisticsTermLeafReader>>& StatisticsTermReader::GetSegmentReaders() const
{
    return _segmentReaders;
}
} // namespace indexlibv2::index
