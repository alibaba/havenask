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
#ifndef __INDEXLIB_DELETION_MAP_READER_H
#define __INDEXLIB_DELETION_MAP_READER_H

#include <memory>
#include <unordered_map>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/deletionmap/deletion_map_writer.h"
#include "indexlib/index/normal/deletionmap/in_mem_deletion_map_reader.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Bitmap.h"

namespace indexlib { namespace index {

class DeletionMapReader
{
public:
    typedef std::pair<docid_t, uint32_t> SegmentMeta;
    typedef std::unordered_map<segmentid_t, SegmentMeta> SegmentMetaMap;

public:
    DeletionMapReader(bool isSharedWriter = false);
    virtual ~DeletionMapReader();

    // for test
    DeletionMapReader(uint32_t totalDocCount, bool isSharedWriter = false);

public:
    bool Open(index_base::PartitionData* partitionData);

    uint32_t GetDeletedDocCount() const;
    virtual uint32_t GetDeletedDocCount(segmentid_t segId) const;

    inline bool Delete(docid_t docId);
    inline bool IsDeleted(docid_t docId) const;

public:
    inline bool Delete(segmentid_t segId, docid_t localDocId);
    inline bool IsDeleted(segmentid_t segId, docid_t localDocId) const;
    docid_t GetInMemBaseDocId() const { return mInMemBaseDocId; }
    const util::ExpandableBitmap* GetSegmentDeletionMap(segmentid_t segId) const;
    const DeletionMapWriter* GetWriter() const { return &mWriter; }
    bool MergeBuildingBitmap(const DeletionMapReader& other);

private:
    void InitSegmentMetaMap(index_base::PartitionData* partitionData);
    docid_t GetBaseDocId(segmentid_t segId) const;

private:
    DeletionMapWriter mWriter;

    util::BitmapPtr mBitmap; // optimize cache
    docid_t mInMemBaseDocId;
    SegmentMetaMap mSegmentMetaMap;
    InMemDeletionMapReaderPtr mInMemDeletionMapReader;

private:
    friend class DeletionMapReaderTest;
    friend class IndexTestUtil;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DeletionMapReader);

inline bool DeletionMapReader::Delete(docid_t docId)
{
    if (docId < mInMemBaseDocId) {
        mBitmap->Set(docId);
    }

    return mWriter.Delete(docId); // include in_mem_reader
}

inline bool DeletionMapReader::Delete(segmentid_t segId, docid_t localDocId)
{
    docid_t baseDocid = GetBaseDocId(segId);
    if (baseDocid == INVALID_DOCID) {
        return false;
    }
    return Delete(baseDocid + localDocId);
}

inline bool DeletionMapReader::IsDeleted(docid_t docId) const
{
    if (unlikely(docId >= (docid_t)mInMemBaseDocId)) {
        return !mInMemDeletionMapReader ? true : mInMemDeletionMapReader->IsDeleted(docId - mInMemBaseDocId);
    }

    assert(mBitmap != NULL);
    return mBitmap->Test(docId);
}

inline bool DeletionMapReader::IsDeleted(segmentid_t segId, docid_t localDocId) const
{
    docid_t baseDocid = GetBaseDocId(segId);
    if (baseDocid == INVALID_DOCID) {
        return true;
    }
    return IsDeleted(baseDocid + localDocId);
}

inline uint32_t DeletionMapReader::GetDeletedDocCount() const
{
    uint32_t deleteCount = 0;
    if (mBitmap) {
        deleteCount += mBitmap->GetSetCount();
    }
    if (mInMemDeletionMapReader) {
        deleteCount += mInMemDeletionMapReader->GetDeletedDocCount();
    }
    return deleteCount;
}

inline docid_t DeletionMapReader::GetBaseDocId(segmentid_t segId) const
{
    if (mInMemDeletionMapReader && unlikely(segId == mInMemDeletionMapReader->GetInMemSegmentId())) {
        return mInMemBaseDocId;
    }

    SegmentMetaMap::const_iterator iter = mSegmentMetaMap.find(segId);
    if (iter != mSegmentMetaMap.end()) {
        const SegmentMeta& segMeta = iter->second;
        return segMeta.first;
    }
    return INVALID_DOCID;
}
}} // namespace indexlib::index

#endif //__INDEXLIB_DELETION_MAP_READER_H
