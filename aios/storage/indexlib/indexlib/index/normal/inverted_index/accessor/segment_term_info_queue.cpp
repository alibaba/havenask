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
#include "indexlib/index/normal/inverted_index/accessor/segment_term_info_queue.h"

#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/OnDiskBitmapIndexIterator.h"
#include "indexlib/index/inverted_index/patch/SingleFieldIndexSegmentPatchIterator.h"
#include "indexlib/index/inverted_index/patch/SingleTermIndexSegmentPatchIterator.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/patch/patch_file_finder.h"
#include "indexlib/index_base/patch/patch_file_finder_creator.h"

using namespace std;

using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::file_system;
using namespace indexlib::index_base;

namespace indexlib::index::legacy {

IE_LOG_SETUP(index, SegmentTermInfoQueue);

SegmentTermInfoQueue::SegmentTermInfoQueue(const IndexConfigPtr& indexConf,
                                           const OnDiskIndexIteratorCreatorPtr& onDiskIndexIterCreator)
    : mIndexConfig(indexConf)
    , mOnDiskIndexIterCreator(onDiskIndexIterCreator)
{
}

SegmentTermInfoQueue::~SegmentTermInfoQueue()
{
    while (!mSegmentTermInfos.empty()) {
        SegmentTermInfo* segTermInfo = mSegmentTermInfos.top();
        mSegmentTermInfos.pop();
        delete segTermInfo;
    }
    for (size_t i = 0; i < mMergingSegmentTermInfos.size(); i++) {
        DELETE_AND_SET_NULL(mMergingSegmentTermInfos[i]);
    }
}

void SegmentTermInfoQueue::Init(const SegmentDirectoryBasePtr& segDir, const SegmentMergeInfos& segMergeInfos)
{
    mSegmentDirectory = segDir;
    for (size_t i = 0; i < segMergeInfos.size(); ++i) {
        AddOnDiskTermInfo(segMergeInfos[i]);
    }
}

void SegmentTermInfoQueue::AddOnDiskTermInfo(const SegmentMergeInfo& segMergeInfo)
{
    std::shared_ptr<IndexIterator> indexIt = CreateOnDiskNormalIterator(segMergeInfo);
    std::shared_ptr<SingleFieldIndexSegmentPatchIterator> patchIter = CreatePatchIterator(segMergeInfo);

    AddQueueItem(segMergeInfo, indexIt, patchIter, SegmentTermInfo::TM_NORMAL);

    if (mIndexConfig->GetHighFreqVocabulary()) {
        indexIt = CreateOnDiskBitmapIterator(segMergeInfo);
        patchIter = CreatePatchIterator(segMergeInfo);
        AddQueueItem(segMergeInfo, indexIt, patchIter, SegmentTermInfo::TM_BITMAP);
    }
}

void SegmentTermInfoQueue::AddQueueItem(const SegmentMergeInfo& segMergeInfo,
                                        const std::shared_ptr<IndexIterator>& indexIt,
                                        const std::shared_ptr<SingleFieldIndexSegmentPatchIterator>& patchIter,
                                        SegmentTermInfo::TermIndexMode mode)
{
    if (indexIt or (patchIter and !patchIter->Empty())) {
        unique_ptr<SegmentTermInfo> queItem(new SegmentTermInfo(mIndexConfig->GetInvertedIndexType(),
                                                                segMergeInfo.segmentId, segMergeInfo.baseDocId, indexIt,
                                                                patchIter, mode));
        if (queItem->Next()) {
            mSegmentTermInfos.push(queItem.release());
        }
    }
}

const SegmentTermInfos& SegmentTermInfoQueue::CurrentTermInfos(index::DictKeyInfo& key,
                                                               SegmentTermInfo::TermIndexMode& termMode)
{
    assert(mMergingSegmentTermInfos.empty());
    SegmentTermInfo* item = mSegmentTermInfos.top();
    mSegmentTermInfos.pop();

    key = item->GetKey();
    termMode = item->GetTermIndexMode();

    mMergingSegmentTermInfos.push_back(item);

    item = (mSegmentTermInfos.size() > 0) ? mSegmentTermInfos.top() : NULL;
    while (item != NULL && key == item->GetKey()) {
        if (item->GetTermIndexMode() != termMode) {
            break;
        }
        mMergingSegmentTermInfos.push_back(item);
        mSegmentTermInfos.pop();
        item = (mSegmentTermInfos.size() > 0) ? mSegmentTermInfos.top() : NULL;
    }
    return mMergingSegmentTermInfos;
}

void SegmentTermInfoQueue::MoveToNextTerm()
{
    for (size_t i = 0; i < mMergingSegmentTermInfos.size(); ++i) {
        SegmentTermInfo* itemPtr = mMergingSegmentTermInfos[i];
        assert(itemPtr);
        if (itemPtr->Next()) {
            mMergingSegmentTermInfos[i] = NULL;
            mSegmentTermInfos.push(itemPtr);
        } else {
            DELETE_AND_SET_NULL(mMergingSegmentTermInfos[i]);
        }
    }
    mMergingSegmentTermInfos.clear();
}

std::shared_ptr<IndexIterator>
SegmentTermInfoQueue::CreateOnDiskNormalIterator(const SegmentMergeInfo& segMergeInfo) const
{
    PartitionDataPtr partitionData = mSegmentDirectory->GetPartitionData();
    assert(partitionData);

    SegmentData segData = partitionData->GetSegmentData(segMergeInfo.segmentId);

    OnDiskIndexIterator* onDiskIndexIter = mOnDiskIndexIterCreator->Create(segData);
    std::shared_ptr<IndexIterator> indexIt;
    if (onDiskIndexIter) {
        indexIt.reset(onDiskIndexIter);
        onDiskIndexIter->Init();
    }
    return indexIt;
}

std::shared_ptr<IndexIterator>
SegmentTermInfoQueue::CreateOnDiskBitmapIterator(const SegmentMergeInfo& segMergeInfo) const
{
    PartitionDataPtr partitionData = mSegmentDirectory->GetPartitionData();
    assert(partitionData);

    SegmentData segData = partitionData->GetSegmentData(segMergeInfo.segmentId);
    DirectoryPtr indexDirectory = segData.GetIndexDirectory(mIndexConfig->GetIndexName(), false);

    std::shared_ptr<IndexIterator> indexIt;
    if (indexDirectory && indexDirectory->IsExist(BITMAP_DICTIONARY_FILE_NAME)) {
        OnDiskIndexIterator* onDiskIndexIter = mOnDiskIndexIterCreator->CreateBitmapIterator(indexDirectory);

        indexIt.reset(onDiskIndexIter);
        onDiskIndexIter->Init();
    }
    return indexIt;
}

std::shared_ptr<SingleFieldIndexSegmentPatchIterator>
SegmentTermInfoQueue::CreatePatchIterator(const index_base::SegmentMergeInfo& segMergeInfo) const
{
    index_base::PatchFileFinderPtr patchFinder =
        index_base::PatchFileFinderCreator::Create(mSegmentDirectory->GetPartitionData().get());
    index_base::PatchFileInfoVec patchVec;
    patchFinder->FindIndexPatchFilesForTargetSegment(mIndexConfig, segMergeInfo.segmentId, &patchVec);

    std::shared_ptr<SingleFieldIndexSegmentPatchIterator> patchIter(
        new SingleFieldIndexSegmentPatchIterator(mIndexConfig, segMergeInfo.segmentId));
    for (size_t i = 0; i < patchVec.size(); i++) {
        auto status = patchIter->AddPatchFile(patchVec[i].patchDirectory->GetIDirectory(), patchVec[i].patchFileName,
                                              patchVec[i].srcSegment, patchVec[i].destSegment);
        if (!status.IsOK()) {
            IE_LOG(ERROR, "Add patch file %s failed.", patchVec[i].patchFileName.c_str());
        }
    }
    return patchIter;
}
} // namespace indexlib::index::legacy
