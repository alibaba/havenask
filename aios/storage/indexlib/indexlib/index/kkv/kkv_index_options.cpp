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
#include "indexlib/index/kkv/kkv_index_options.h"

#include "indexlib/framework/LevelInfo.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::index_base;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, KKVIndexOptions);

void KKVIndexOptions::Init(const KKVIndexConfigPtr& _kkvConfig, const PartitionDataPtr& partitionData,
                           int64_t latestNeedSkipIncTs)
{
    KVIndexOptions::Init(_kkvConfig, partitionData, latestNeedSkipIncTs);
    kkvConfig = _kkvConfig;
    if (kkvConfig->EnableSuffixKeyKeepSortSequence() && MatchSortCondition(partitionData)) {
        sortParams = kkvConfig->GetSuffixKeyTruncateParams();
    }
    kkvIndexPreference = kkvConfig->GetIndexPreference();
}

bool KKVIndexOptions::MatchSortCondition(const PartitionDataPtr& partitionData) const
{
    SegmentIteratorPtr buildingSegIter = partitionData->CreateSegmentIterator()->CreateIterator(SIT_BUILDING);
    assert(buildingSegIter);
    if (buildingSegIter->IsValid()) {
        return false;
    }

    Version version = partitionData->GetVersion();
    const indexlibv2::framework::LevelInfo& levelInfo = version.GetLevelInfo();
    size_t columnCount = levelInfo.GetShardCount();
    for (size_t i = 0; i < columnCount; ++i) {
        vector<segmentid_t> segIds = levelInfo.GetSegmentIds(i);
        if (segIds.size() > 1) {
            return false;
        }
    }
    return true;
}
}} // namespace indexlib::index
