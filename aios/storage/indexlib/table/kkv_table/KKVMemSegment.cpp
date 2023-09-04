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
#include "indexlib/table/kkv_table/KKVMemSegment.h"

#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/kkv/Constant.h"
#include "indexlib/index/kkv/common/KKVSegmentStatistics.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, KKVMemSegment);

KKVMemSegment::KKVMemSegment(const config::TabletOptions* options, const std::shared_ptr<config::ITabletSchema>& schema,
                             const framework::SegmentMeta& segmentMeta)
    : plain::PlainMemSegment(options, schema, segmentMeta)
{
}

const std::vector<std::shared_ptr<indexlibv2::index::IMemIndexer>>& KKVMemSegment::GetBuildIndexers()
{
    if (_hasCheckPKValueIndex) {
        return _indexers;
    }
    _indexers.clear();
    for (const auto& [indexMapKey, indexerAndMemUpdater] : _indexMap) {
        auto& indexName = indexMapKey.second;
        auto& memIndexer = indexerAndMemUpdater.first;
        if (indexlibv2::index::KKV_RAW_KEY_INDEX_NAME == indexName) {
            _indexers.insert(_indexers.begin(), memIndexer);
        } else {
            _indexers.emplace_back(memIndexer);
        }
    }
    _hasCheckPKValueIndex = true;
    return _indexers;
}

std::shared_ptr<plain::PlainMemSegment>
KKVMemSegment::CreatePlainMemSegment(const config::TabletOptions* options,
                                     const std::shared_ptr<config::ITabletSchema>& schema,
                                     const framework::SegmentMeta& segmentMeta)
{
    return std::make_shared<KKVMemSegment>(options, schema, segmentMeta);
}

std::pair<Status, size_t> KKVMemSegment::GetLastSegmentMemUse(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                              const indexlib::framework::SegmentMetrics* metrics) const
{
    std::string targetGroupName =
        index::KKVSegmentStatistics::GetTargetGroupNameLegacy(indexConfig->GetIndexName(), metrics);
    size_t totalMemoryUse = 0;
    if (!metrics->Get<size_t>(targetGroupName, index::KKVSegmentStatistics::KKV_SEGMENT_MEM_USE, totalMemoryUse)) {
        return std::make_pair(Status::NotFound(), totalMemoryUse);
    }
    return std::make_pair(Status::OK(), totalMemoryUse);
}

} // namespace indexlibv2::table
