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
#include "indexlib/index/normal/deletionmap/deletion_map_writer.h"

#include "autil/StringUtil.h"
#include "fslib/fslib.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/normal/deletionmap/deletion_map_segment_writer.h"
#include "indexlib/index/normal/deletionmap/in_mem_deletion_map_reader.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/patch/patch_file_finder.h"
#include "indexlib/index_base/patch/patch_file_finder_creator.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/in_memory_segment_reader.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_define.h"
#include "indexlib/util/ExpandableBitmap.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;
using namespace autil::mem_pool;

using namespace indexlib::index_base;
using namespace indexlib::util;
using namespace indexlib::index;
using namespace indexlib::file_system;
using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, DeletionMapWriter);

DeletionMapWriter::DeletionMapWriter(bool needCopy) : mNeedCopy(needCopy) {}

DeletionMapWriter::~DeletionMapWriter() {}

void DeletionMapWriter::Init(PartitionData* partitionData)
{
    IE_LOG(INFO, "init deletion map begin");
    DeletePatchInfos patchInfos;
    PatchFileFinderPtr patchFinder = PatchFileFinderCreator::Create(partitionData);
    patchFinder->FindDeletionMapFiles(patchInfos);

    PartitionSegmentIteratorPtr iter = partitionData->CreateSegmentIterator();
    assert(iter);
    while (iter->IsValid()) {
        if (iter->GetType() == SIT_BUILT) {
            const SegmentData& segmentData = iter->GetSegmentData();
            if (segmentData.GetSegmentInfo()->docCount == 0) {
                iter->MoveToNext();
                continue;
            }

            DeletionMapSegmentWriterPtr segmentWriter;
            DeletePatchInfos::iterator patchIter = patchInfos.find(segmentData.GetSegmentId());
            if (patchIter == patchInfos.end()) {
                segmentWriter = CreateInMemSharedSegmentWriter(segmentData);
            } else {
                segmentWriter.reset(new DeletionMapSegmentWriter);
                PatchFileInfo patchFileInfo = patchIter->second;
                segmentWriter->Init(patchFileInfo.patchDirectory, patchFileInfo.patchFileName, mNeedCopy);
            }
            mSegmentWriters.push_back(make_pair(segmentData, segmentWriter));
            mSegIdToIdx.insert(make_pair(segmentData.GetSegmentId(), mSegmentWriters.size() - 1));
        } else {
            assert(iter->GetType() == SIT_BUILDING);
            InMemorySegmentPtr inMemSegment = iter->GetInMemSegment();
            assert(inMemSegment);
            if (inMemSegment->GetStatus() != InMemorySegment::BUILDING) {
                SegmentData segData = iter->GetSegmentData();
                if (segData.GetSegmentInfo()->docCount == 0) {
                    iter->MoveToNext();
                    continue;
                }
                assert(inMemSegment);
                // DUMPING segments
                DeletionMapSegmentWriterPtr segmentWriter = CreateDumpingDeletionMapSegmentWriter(inMemSegment);
                mSegmentWriters.push_back(make_pair(iter->GetSegmentData(), segmentWriter));
                mSegIdToIdx.insert(make_pair(segData.GetSegmentId(), mSegmentWriters.size() - 1));
            } else {
                assert(!mBuildingSegmentReader);
                mBuildingSegmentReader = inMemSegment->GetSegmentReader()->GetInMemDeletionMapReader();
            }
        }
        iter->MoveToNext();
    }
    IE_LOG(INFO, "init deletion map end");
}

DeletionMapSegmentWriterPtr DeletionMapWriter::GetSegmentWriter(segmentid_t segId) const
{
    const auto it = mSegIdToIdx.find(segId);
    if (it == mSegIdToIdx.end()) {
        return DeletionMapSegmentWriterPtr();
    }
    return mSegmentWriters[it->second].second;
}

const ExpandableBitmap* DeletionMapWriter::GetSegmentDeletionMap(segmentid_t segId) const
{
    if (mBuildingSegmentReader && segId == mBuildingSegmentReader->GetInMemSegmentId()) {
        return mBuildingSegmentReader->GetBitmap();
    }
    const auto it = mSegIdToIdx.find(segId);
    if (it == mSegIdToIdx.end()) {
        return NULL;
    }
    return mSegmentWriters[it->second].second->GetBitmap();
}

bool DeletionMapWriter::Delete(docid_t docId)
{
    docid_t nextBaseDocId = 0;
    for (vector<SegmentWriter>::iterator it = mSegmentWriters.begin(); it != mSegmentWriters.end(); ++it) {
        const SegmentData& segData = it->first;
        docid_t baseDocid = segData.GetBaseDocId();
        nextBaseDocId = baseDocid + segData.GetSegmentInfo()->docCount;

        if (docId < nextBaseDocId) {
            it->second->Delete(docId - baseDocid);
            return true;
        }
    }

    if (mBuildingSegmentReader) {
        mBuildingSegmentReader->Delete(docId - nextBaseDocId);
        return true;
    }
    return false;
}

void DeletionMapWriter::Dump(const file_system::DirectoryPtr& directory)
{
    file_system::DirectoryPtr deletionmapDirectory = directory->GetDirectory(DELETION_MAP_DIR_NAME, false);
    if (!deletionmapDirectory) {
        deletionmapDirectory = directory->MakeDirectory(DELETION_MAP_DIR_NAME);
    }
    for (vector<SegmentWriter>::iterator it = mSegmentWriters.begin(); it != mSegmentWriters.end(); ++it) {
        const SegmentData& segData = it->first;
        it->second->Dump(deletionmapDirectory, segData.GetSegmentId());
    }
}

bool DeletionMapWriter::IsDirty() const
{
    for (vector<SegmentWriter>::const_iterator it = mSegmentWriters.begin(); it != mSegmentWriters.end(); ++it) {
        if (it->second->IsDirty()) {
            return true;
        }
    }
    return false;
}

BitmapPtr DeletionMapWriter::CreateGlobalBitmap()
{
    IE_LOG(INFO, "create global bitmap begin");
    uint32_t totalDocCount = 0;
    if (!mSegmentWriters.empty()) {
        const SegmentData& lastSegData = mSegmentWriters.rbegin()->first;
        totalDocCount = lastSegData.GetBaseDocId() + lastSegData.GetSegmentInfo()->docCount;
    }

    BitmapPtr bitmap(new Bitmap(totalDocCount));
    for (size_t i = 0; i < mSegmentWriters.size(); i++) {
        const SegmentData& segData = mSegmentWriters[i].first;
        uint32_t docCount = segData.GetSegmentInfo()->docCount;
        if (docCount == 0) {
            continue;
        }

        docid_t baseDocid = segData.GetBaseDocId();
        const DeletionMapSegmentWriterPtr& segWriter = mSegmentWriters[i].second;
        assert(segWriter);
        bitmap->Copy(baseDocid, segWriter->GetData(), docCount);
    }
    IE_LOG(INFO, "create global bitmap end");
    return bitmap;
}

DeletionMapSegmentWriterPtr DeletionMapWriter::CreateInMemSharedSegmentWriter(const SegmentData& segData)
{
    segmentid_t segId = segData.GetSegmentId();
    const std::shared_ptr<const SegmentInfo>& segInfo = segData.GetSegmentInfo();
    DirectoryPtr segmentDirectory = segData.GetDirectory();
    assert(segmentDirectory);

    DirectoryPtr deletionmapDirectory = segmentDirectory->GetDirectory(DELETION_MAP_DIR_NAME, true);
    DeletionMapSegmentWriterPtr segWriter(new DeletionMapSegmentWriter);
    segWriter->Init(deletionmapDirectory, segId, segInfo->docCount, mNeedCopy);
    return segWriter;
}

void DeletionMapWriter::GetPatchedSegmentIds(vector<segmentid_t>& patchSegIds) const
{
    for (vector<SegmentWriter>::const_iterator it = mSegmentWriters.begin(); it != mSegmentWriters.end(); ++it) {
        if (it->second->IsDirty()) {
            patchSegIds.push_back(it->first.GetSegmentId());
        }
    }
}

DeletionMapSegmentWriterPtr
DeletionMapWriter::CreateDumpingDeletionMapSegmentWriter(const InMemorySegmentPtr& inMemSegment)
{
    DeletionMapSegmentWriterPtr segWriter(new DeletionMapSegmentWriter);
    segWriter->Init(inMemSegment, mNeedCopy);
    return segWriter;
}
}} // namespace indexlib::index
