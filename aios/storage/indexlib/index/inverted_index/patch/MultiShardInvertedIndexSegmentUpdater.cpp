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
#include "indexlib/index/inverted_index/patch/MultiShardInvertedIndexSegmentUpdater.h"

#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/inverted_index/format/ShardingIndexHasher.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, MultiShardInvertedIndexSegmentUpdater);

MultiShardInvertedIndexSegmentUpdater::MultiShardInvertedIndexSegmentUpdater(
    segmentid_t segId, const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig)
    : IInvertedIndexSegmentUpdater(segId, indexConfig)
{
    assert(indexConfig->GetShardingType() ==
           indexlibv2::config::InvertedIndexConfig::IndexShardingType::IST_NEED_SHARDING);
    assert(!indexConfig->GetShardingIndexConfigs().empty());
    for (auto config : indexConfig->GetShardingIndexConfigs()) {
        auto updater = std::make_unique<InvertedIndexSegmentUpdater>(segId, config);
        _shardingUpdaters.push_back(std::move(updater));
    }
    _indexHasher.reset(new ShardingIndexHasher());
    _indexHasher->Init(indexConfig);
}

void MultiShardInvertedIndexSegmentUpdater::Update(docid_t docId, const document::ModifiedTokens& modifiedTokens)
{
    assert(docId >= 0);
    for (size_t i = 0; i < modifiedTokens.NonNullTermSize(); ++i) {
        uint64_t termKey = modifiedTokens[i].first;
        size_t shardingIdx = _indexHasher->GetShardingIdx(DictKeyInfo(termKey));
        assert(shardingIdx < _shardingUpdaters.size());
        _shardingUpdaters[shardingIdx]->Update(docId, DictKeyInfo(termKey),
                                               modifiedTokens[i].second == document::ModifiedTokens::Operation::REMOVE);
    }
    document::ModifiedTokens::Operation nullTermOp;
    if (modifiedTokens.GetNullTermOperation(&nullTermOp)) {
        if (nullTermOp != document::ModifiedTokens::Operation::NONE) {
            size_t shardingIdx = _indexHasher->GetShardingIdxForNullTerm();
            _shardingUpdaters[shardingIdx]->Update(docId, DictKeyInfo::NULL_TERM,
                                                   nullTermOp == document::ModifiedTokens::Operation::REMOVE);
        }
    }
}

void MultiShardInvertedIndexSegmentUpdater::Update(docid_t localDocId, DictKeyInfo termKey, bool isDelete)
{
    size_t shardingIdx = _indexHasher->GetShardingIdx(termKey);
    _shardingUpdaters[shardingIdx]->Update(localDocId, termKey, isDelete);
}

Status MultiShardInvertedIndexSegmentUpdater::Dump(const std::shared_ptr<file_system::IDirectory>& indexesDir,
                                                   segmentid_t srcSegment)
{
    for (auto& updater : _shardingUpdaters) {
        auto status = updater->Dump(indexesDir, srcSegment);
        RETURN_IF_STATUS_ERROR(status, "dump failed");
    }
    return Status::OK();
}

} // namespace indexlib::index
