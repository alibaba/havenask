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

#include <memory>
#include <unordered_map>

#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/inverted_index/format/ShardingIndexHasher.h"
#include "indexlib/index/normal/inverted_index/accessor/patch_index_segment_updater.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/SimplePool.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

DECLARE_REFERENCE_CLASS(document, ModifiedTokens);

namespace indexlib { namespace index {

class MultiShardingPatchIndexSegmentUpdater : public PatchIndexSegmentUpdaterBase
{
public:
    MultiShardingPatchIndexSegmentUpdater(util::BuildResourceMetrics* buildResourceMetrics, segmentid_t segId,
                                          const config::IndexConfigPtr& indexConfig);

public:
    void Update(docid_t docId, const document::ModifiedTokens& modifiedTokens) override;
    void Dump(const file_system::DirectoryPtr& indexDir, segmentid_t srcSegment) override;

private:
    std::vector<std::unique_ptr<PatchIndexSegmentUpdater>> _shardingUpdaters;
    ShardingIndexHasher _shardingIndexHasher;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiShardingPatchIndexSegmentUpdater);
}} // namespace indexlib::index
