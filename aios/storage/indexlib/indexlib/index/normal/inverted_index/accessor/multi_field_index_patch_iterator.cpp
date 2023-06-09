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
#include "indexlib/index/normal/inverted_index/accessor/multi_field_index_patch_iterator.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/inverted_index/patch/IndexUpdateTermIterator.h"
#include "indexlib/index/normal/inverted_index/accessor/single_field_index_patch_iterator.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/partition_data.h"

namespace indexlib { namespace index {
IE_LOG_SETUP(index, MultiFieldIndexPatchIterator);

void MultiFieldIndexPatchIterator::Init(const index_base::PartitionDataPtr& partitionData,
                                        bool ignorePatchToOldIncSegment, const index_base::Version& lastLoadVersion,
                                        segmentid_t startLoadSegment, bool isSub)
{
    _isSub = isSub;
    const config::IndexSchemaPtr& indexSchema = _schema->GetIndexSchema();
    if (!indexSchema) {
        return;
    }

    auto indexConfigs = indexSchema->CreateIterator();
    for (auto iter = indexConfigs->Begin(); iter != indexConfigs->End(); ++iter) {
        const config::IndexConfigPtr& indexConfig = *iter;
        if (!indexConfig->IsIndexUpdatable()) {
            continue;
        }
        if (indexConfig->GetShardingType() == config::IndexConfig::IST_NEED_SHARDING) {
            continue;
        }
        AddSingleFieldIterator(indexConfig, partitionData, ignorePatchToOldIncSegment, startLoadSegment, _isSub);
    }
    if (!_singleFieldPatchIters.empty()) {
        _cursor = 0;
        Log();
    }
}

void MultiFieldIndexPatchIterator::Log() const
{
    if (_cursor < _singleFieldPatchIters.size()) {
        IE_LOG(INFO, "next index patch field [%s], patch item[%lu]",
               _singleFieldPatchIters[_cursor]->GetIndexName().c_str(),
               _singleFieldPatchIters[_cursor]->GetPatchItemCount());
    }
}

void MultiFieldIndexPatchIterator::AddSingleFieldIterator(const config::IndexConfigPtr& indexConfig,
                                                          const index_base::PartitionDataPtr& partitionData,
                                                          bool ignorePatchToOldIncSegment, segmentid_t startLoadSegment,
                                                          bool isSub)
{
    std::unique_ptr<SingleFieldIndexPatchIterator> indexPatchIter(
        new SingleFieldIndexPatchIterator(indexConfig, isSub));
    indexPatchIter->Init(partitionData, ignorePatchToOldIncSegment, startLoadSegment);
    _patchLoadExpandSize += indexPatchIter->GetPatchLoadExpandSize();
    _singleFieldPatchIters.push_back(std::move(indexPatchIter));
}

std::unique_ptr<IndexUpdateTermIterator> MultiFieldIndexPatchIterator::Next()
{
    if (_cursor < 0) {
        return nullptr;
    }
    while (_cursor < _singleFieldPatchIters.size()) {
        auto& fieldIter = _singleFieldPatchIters[_cursor];
        std::unique_ptr<IndexUpdateTermIterator> r = fieldIter->Next();
        if (r) {
            r->SetIsSub(_isSub);
            return r;
        } else {
            ++_cursor;
            Log();
        }
    }
    return nullptr;
}

void MultiFieldIndexPatchIterator::CreateIndependentPatchWorkItems(std::vector<PatchWorkItem*>& workItems)
{
    if (_cursor < 0) {
        return;
    }
    for (; _cursor < _singleFieldPatchIters.size(); ++_cursor) {
        _singleFieldPatchIters[_cursor]->CreateIndependentPatchWorkItems(workItems);
    }
}
}} // namespace indexlib::index
