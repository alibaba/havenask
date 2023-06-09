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
#include "indexlib/index/normal/inverted_index/accessor/single_field_index_patch_iterator.h"

#include <sstream>

#include "indexlib/file_system/Directory.h"
#include "indexlib/index/inverted_index/patch/SingleFieldIndexSegmentPatchIterator.h"
#include "indexlib/index/inverted_index/patch/SingleTermIndexSegmentPatchIterator.h"
#include "indexlib/index/normal/inverted_index/accessor/index_patch_work_item.h"
#include "indexlib/index_base/patch/patch_file_filter.h"
#include "indexlib/index_base/patch/patch_file_finder_creator.h"

namespace indexlib { namespace index {
IE_LOG_SETUP(index, SingleFieldIndexPatchIterator);

void SingleFieldIndexPatchIterator::Init(const index_base::PartitionDataPtr& partitionData,
                                         bool isIncConsistentWithRealtime, segmentid_t startLoadSegment)
{
    index_base::PatchInfos patchInfos;
    index_base::PatchFileFinderPtr patchFinder = index_base::PatchFileFinderCreator::Create(partitionData.get());

    patchFinder->FindIndexPatchFiles(_indexConfig, &patchInfos);

    index_base::PatchFileFilter patchFilter(partitionData, isIncConsistentWithRealtime, startLoadSegment);
    index_base::PatchInfos validPatchInfos = patchFilter.Filter(patchInfos);
    for (auto iter = validPatchInfos.begin(); iter != validPatchInfos.end(); iter++) {
        auto segPatchIter = CreateSegmentPatchIterator(iter->second, iter->first);
        _patchLoadExpandSize += segPatchIter->GetPatchLoadExpandSize();
        _patchItemCount += segPatchIter->GetPatchItemCount();
        _segmentIterators.push_back(segPatchIter);
    }
    if (!_segmentIterators.empty()) {
        _cursor = 0;
        Log();
    }
}

void SingleFieldIndexPatchIterator::Log() const
{
    if (_cursor < _segmentIterators.size()) {
        auto& segIter = _segmentIterators[_cursor];
        IE_LOG(INFO, "index [%s] patch next target segment [%d], patch item[%lu]", _indexConfig->GetIndexName().c_str(),
               segIter->GetTargetSegmentId(), segIter->GetPatchItemCount());
    }
}

std::shared_ptr<SingleFieldIndexSegmentPatchIterator>
SingleFieldIndexPatchIterator::CreateSegmentPatchIterator(const index_base::PatchFileInfoVec& patchInfoVec,
                                                          segmentid_t targetSegment)
{
    std::shared_ptr<SingleFieldIndexSegmentPatchIterator> patchIter(
        new SingleFieldIndexSegmentPatchIterator(_indexConfig, targetSegment));
    for (size_t i = 0; i < patchInfoVec.size(); i++) {
        auto status =
            patchIter->AddPatchFile(patchInfoVec[i].patchDirectory->GetIDirectory(), patchInfoVec[i].patchFileName,
                                    patchInfoVec[i].srcSegment, patchInfoVec[i].destSegment);
        if (!status.IsOK()) {
            IE_LOG(ERROR, "Add patch file %s failed.", patchInfoVec[i].patchFileName.c_str());
        }
    }
    return patchIter;
}

std::unique_ptr<IndexUpdateTermIterator> SingleFieldIndexPatchIterator::Next()
{
    if (_cursor < 0) {
        return nullptr;
    }
    while (_cursor < _segmentIterators.size()) {
        auto& segIter = _segmentIterators[_cursor];
        std::unique_ptr<IndexUpdateTermIterator> r(segIter->NextTerm());
        if (r) {
            return r;
        } else {
            ++_cursor;
            Log();
        }
    }
    return nullptr;
}

void SingleFieldIndexPatchIterator::CreateIndependentPatchWorkItems(std::vector<PatchWorkItem*>& workItems)
{
    if (_cursor < 0) {
        return;
    }
    for (; _cursor < _segmentIterators.size(); ++_cursor) {
        const auto& segIter = _segmentIterators[_cursor];
        std::stringstream ss;
        ss << "__index__" << _indexConfig->GetIndexName() << "_" << segIter->GetTargetSegmentId() << "_";
        if (_isSub) {
            ss << "sub";
        } else {
            ss << "main";
        }
        auto workItem =
            new IndexPatchWorkItem(ss.str(), segIter->GetPatchItemCount(), _isSub, PatchWorkItem::PWIT_INDEX, segIter);
        workItems.push_back(workItem);
    }
}
}} // namespace indexlib::index
