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
#pragma once

#include "autil/Log.h"
#include "indexlib/index/inverted_index/patch/InvertedIndexSegmentUpdater.h"

namespace indexlib::index {
class ShardingIndexHasher;

class MultiShardInvertedIndexSegmentUpdater : public IInvertedIndexSegmentUpdater
{
public:
    MultiShardInvertedIndexSegmentUpdater(segmentid_t segId,
                                          const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig);
    ~MultiShardInvertedIndexSegmentUpdater() = default;

public:
    void Update(docid_t docId, const document::ModifiedTokens& modifiedTokens) override;
    void Update(docid_t localDocId, DictKeyInfo termKey, bool isDelete) override;
    Status Dump(const std::shared_ptr<file_system::IDirectory>& indexesDir, segmentid_t srcSegment) override;

private:
    std::vector<std::unique_ptr<InvertedIndexSegmentUpdater>> _shardingUpdaters;
    std::unique_ptr<ShardingIndexHasher> _indexHasher;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
