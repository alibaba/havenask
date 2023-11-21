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
#include "indexlib/table/kv_table/KVMemSegment.h"

#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/kv/Common.h"
#include "indexlib/index/kv/SegmentStatistics.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, KVMemSegment);

KVMemSegment::KVMemSegment(const config::TabletOptions* options, const std::shared_ptr<config::ITabletSchema>& schema,
                           const framework::SegmentMeta& segmentMeta, bool enableMemoryReclaim)
    : plain::PlainMemSegment(options, schema, segmentMeta)
    , _enableMemoryReclaim(enableMemoryReclaim)
{
}

const std::vector<std::shared_ptr<indexlibv2::index::IMemIndexer>>& KVMemSegment::GetBuildIndexers()
{
    if (_hasCheckPKValueIndex) {
        return _indexers;
    }
    _indexers.clear();
    for (const auto& [indexMapKey, indexerAndMemUpdater] : _indexMap) {
        auto& indexName = indexMapKey.second;
        auto& memIndexer = indexerAndMemUpdater.first;
        if (indexlibv2::index::KV_RAW_KEY_INDEX_NAME == indexName) {
            _indexers.insert(_indexers.begin(), memIndexer);
        } else {
            _indexers.emplace_back(memIndexer);
        }
    }
    _hasCheckPKValueIndex = true;
    return _indexers;
}

std::pair<Status, size_t> KVMemSegment::GetLastSegmentMemUse(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                             const indexlib::framework::SegmentMetrics* metrics) const
{
    std::string targetGroupName =
        index::SegmentStatistics::GetTargetGroupNameLegacy(indexConfig->GetIndexName(), metrics);
    size_t totalMemoryUse = 0;
    if (!metrics->Get<size_t>(targetGroupName, index::SegmentStatistics::KV_SEGMENT_MEM_USE, totalMemoryUse)) {
        return std::make_pair(Status::NotFound(), totalMemoryUse);
    }
    return std::make_pair(Status::OK(), totalMemoryUse);
}

void KVMemSegment::PrepareIndexerParameter(const framework::BuildResource& resource, int64_t maxMemoryUseInBytes,
                                           index::MemIndexerParameter& indexerParam) const
{
    plain::PlainMemSegment::PrepareIndexerParameter(resource, maxMemoryUseInBytes, indexerParam);
    if (!_enableMemoryReclaim && indexerParam.indexMemoryReclaimer != nullptr) {
        indexerParam.indexMemoryReclaimer = nullptr;
        AUTIL_LOG(INFO, "disable kv tablet memory reclaim");
    }
}

} // namespace indexlibv2::table
