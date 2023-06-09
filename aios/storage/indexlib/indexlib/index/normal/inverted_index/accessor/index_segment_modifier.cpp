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
#include "indexlib/index/normal/inverted_index/accessor/index_segment_modifier.h"

#include "indexlib/common/dump_item.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_sharding_patch_index_segment_updater.h"
#include "indexlib/index/normal/inverted_index/accessor/patch_index_segment_updater.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

namespace indexlib { namespace index {
IE_LOG_SETUP(index, IndexSegmentModifier);

class IndexSegmentModifierDumpItem : public common::DumpItem
{
public:
    IndexSegmentModifierDumpItem(const file_system::DirectoryPtr& directory,
                                 const PatchIndexSegmentUpdaterBasePtr& updater, segmentid_t srcSegmentId)
        : _directory(directory)
        , _updater(updater)
        , _srcSegId(srcSegmentId)
    {
    }

    ~IndexSegmentModifierDumpItem() {}

public:
    void process(autil::mem_pool::PoolBase* pool) override { _updater->Dump(_directory, _srcSegId); }

    void destroy() override { delete this; }

    void drop() override { destroy(); }

private:
    file_system::DirectoryPtr _directory;
    PatchIndexSegmentUpdaterBasePtr _updater;
    segmentid_t _srcSegId;
};

IndexSegmentModifier::IndexSegmentModifier(segmentid_t segId, const config::IndexSchemaPtr& indexSchema,
                                           util::BuildResourceMetrics* buildResourceMetrics)
    : _indexSchema(indexSchema)
    , _segmentId(segId)
    , _buildResourceMetrics(buildResourceMetrics)
{
}

void IndexSegmentModifier::Update(docid_t docId, indexid_t indexId, const document::ModifiedTokens& modifiedTokens)
{
    GetUpdater(indexId)->Update(docId, modifiedTokens);
}

void IndexSegmentModifier::Dump(const file_system::DirectoryPtr& dir, segmentid_t srcSegment)
{
    for (const auto& updater : _updaters) {
        if (updater) {
            updater->Dump(dir, srcSegment);
        }
    }
}

void IndexSegmentModifier::CreateDumpItems(const file_system::DirectoryPtr& directory, segmentid_t srcSegmentId,
                                           std::vector<std::unique_ptr<common::DumpItem>>* dumpItems)
{
    for (const auto& updater : _updaters) {
        if (updater) {
            dumpItems->push_back(std::make_unique<IndexSegmentModifierDumpItem>(directory, updater, srcSegmentId));
        }
    }
}

const PatchIndexSegmentUpdaterBasePtr& IndexSegmentModifier::GetUpdater(indexid_t indexId)
{
    if (indexId >= _updaters.size()) {
        _updaters.resize(indexId + 1);
    }

    if (!_updaters[indexId]) {
        _updaters[indexId] = CreateUpdater(indexId);
    }
    return _updaters[indexId];
}

PatchIndexSegmentUpdaterBasePtr IndexSegmentModifier::CreateUpdater(indexid_t indexId)
{
    const auto& indexConfig = _indexSchema->GetIndexConfig(indexId);
    if (indexConfig->GetShardingType() == config::IndexConfig::IST_NO_SHARDING) {
        return std::make_shared<PatchIndexSegmentUpdater>(_buildResourceMetrics, _segmentId, indexConfig);
    } else if (indexConfig->GetShardingType() == config::IndexConfig::IST_NEED_SHARDING) {
        return std::make_shared<MultiShardingPatchIndexSegmentUpdater>(_buildResourceMetrics, _segmentId, indexConfig);
    } else {
        assert(false);
        return nullptr;
    }
}
}} // namespace indexlib::index
