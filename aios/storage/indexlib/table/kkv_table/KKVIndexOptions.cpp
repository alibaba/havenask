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
#include "indexlib/table/kkv_table/KKVIndexOptions.h"

#include "indexlib/index/common/field_format/pack_attribute/PackAttributeFormatter.h"

namespace indexlibv2::table {

Status KKVIndexOptions::Init(std::shared_ptr<indexlibv2::config::KKVIndexConfig>& indexConfig,
                             const framework::TabletData* tabletData)
{
    _indexConfig = indexConfig;

    int64_t ttl = indexConfig->GetTTL();
    _ttl = (ttl == indexlib::INVALID_TTL ? indexlib::DEFAULT_TIME_TO_LIVE : ttl);
    _fixedValueLen = indexConfig->GetValueConfig()->GetFixedLength();

    const auto& valueConfig = indexConfig->GetValueConfig();
    auto [status, attributeConfig] = valueConfig->CreatePackAttributeConfig();
    if (!status.IsOK()) {
        return status;
    }

    _plainFormatEncoder.reset(index::PackAttributeFormatter::CreatePlainFormatEncoder(attributeConfig));

    if (indexConfig->EnableSuffixKeyKeepSortSequence() && MatchSortCondition(tabletData)) {
        _sortParams = indexConfig->GetSuffixKeyTruncateParams();
    }

    return Status::OK();
}

bool KKVIndexOptions::MatchSortCondition(const framework::TabletData* tabletData) const
{
    auto slice = tabletData->CreateSlice(framework::Segment::SegmentStatus::ST_BUILDING);
    if (!slice.empty()) {
        return false;
    }

    // TODO (qisa.cb) version vs on_disk_version?
    const auto& version = tabletData->GetOnDiskVersion();
    const auto segmentDescriptions = version.GetSegmentDescriptions();
    const auto& levelInfo = segmentDescriptions->GetLevelInfo();
    // TODO(qisa.cb) 为什么会为空？
    if (!levelInfo) {
        return false;
    }
    size_t shardCount = levelInfo->GetShardCount();
    for (size_t shardIdx = 0; shardIdx < shardCount; ++shardIdx) {
        auto segIds = levelInfo->GetSegmentIds(shardIdx);
        if (segIds.size() > 1) {
            return false;
        }
    }
    return true;
}

} // namespace indexlibv2::table
