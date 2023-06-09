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
#include "indexlib/index/normal/inverted_index/accessor/multi_sharding_patch_index_segment_updater.h"

#include "indexlib/document/index_document/normal_document/modified_tokens.h"
#include "indexlib/file_system/file/SnappyCompressFileWriter.h"
#include "indexlib/index/common/DictKeyInfo.h"
#include "indexlib/index/inverted_index/format/PatchFormat.h"
#include "indexlib/index_define.h"
#include "indexlib/util/Algorithm.h"

namespace indexlib { namespace index {
IE_LOG_SETUP(Index, MultiShardingPatchIndexSegmentUpdater);

MultiShardingPatchIndexSegmentUpdater::MultiShardingPatchIndexSegmentUpdater(
    util::BuildResourceMetrics* buildResourceMetrics, segmentid_t segId, const config::IndexConfigPtr& indexConfig)
{
    if (indexConfig->GetShardingType() != config::IndexConfig::IST_NEED_SHARDING) {
        INDEXLIB_FATAL_ERROR(BadParameter, "index [%s] should be need_sharding index",
                             indexConfig->GetIndexName().c_str());
    }

    const std::vector<config::IndexConfigPtr>& shardingIndexConfigs = indexConfig->GetShardingIndexConfigs();
    if (shardingIndexConfigs.empty()) {
        INDEXLIB_FATAL_ERROR(BadParameter, "index [%s] has none sharding indexConfigs",
                             indexConfig->GetIndexName().c_str());
    }

    for (size_t i = 0; i < shardingIndexConfigs.size(); ++i) {
        auto updater = std::make_unique<PatchIndexSegmentUpdater>(buildResourceMetrics, segId, shardingIndexConfigs[i]);
        _shardingUpdaters.push_back(std::move(updater));
    }
    _shardingIndexHasher.Init(indexConfig);
}

void MultiShardingPatchIndexSegmentUpdater::Update(docid_t docId, const document::ModifiedTokens& modifiedTokens)
{
    assert(docId >= 0);
    for (size_t i = 0; i < modifiedTokens.NonNullTermSize(); ++i) {
        uint64_t termKey = modifiedTokens[i].first;
        size_t shardingIdx = _shardingIndexHasher.GetShardingIdx(index::DictKeyInfo(termKey));
        assert(shardingIdx < _shardingUpdaters.size());
        _shardingUpdaters[shardingIdx]->Update(docId, index::DictKeyInfo(termKey),
                                               modifiedTokens[i].second == document::ModifiedTokens::Operation::REMOVE);
    }
    document::ModifiedTokens::Operation nullTermOp;
    if (modifiedTokens.GetNullTermOperation(&nullTermOp)) {
        if (nullTermOp != document::ModifiedTokens::Operation::NONE) {
            size_t shardingIdx = _shardingIndexHasher.GetShardingIdxForNullTerm();
            _shardingUpdaters[shardingIdx]->Update(docId, index::DictKeyInfo::NULL_TERM,
                                                   nullTermOp == document::ModifiedTokens::Operation::REMOVE);
        }
    }
    for (auto& updater : _shardingUpdaters) {
        updater->UpdateBuildResourceMetrics();
    }
}

void MultiShardingPatchIndexSegmentUpdater::Dump(const file_system::DirectoryPtr& indexDir, segmentid_t srcSegment)
{
    for (auto& updater : _shardingUpdaters) {
        updater->Dump(indexDir, srcSegment);
    }
}
}} // namespace indexlib::index
