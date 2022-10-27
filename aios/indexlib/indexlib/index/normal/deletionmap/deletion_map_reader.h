#ifndef __INDEXLIB_DELETION_MAP_READER_H
#define __INDEXLIB_DELETION_MAP_READER_H

#include <tr1/memory>
#include <tr1/unordered_map>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/bitmap.h"
#include "indexlib/index/normal/deletionmap/in_mem_deletion_map_reader.h"
#include "indexlib/index/normal/deletionmap/deletion_map_writer.h"

IE_NAMESPACE_BEGIN(index);

class DeletionMapReader
{
public:
    typedef std::pair<docid_t, uint32_t> SegmentMeta;
    typedef std::tr1::unordered_map<segmentid_t, SegmentMeta> SegmentMetaMap;

public:
    DeletionMapReader();
    virtual ~DeletionMapReader();

    // for test
    DeletionMapReader(uint32_t totalDocCount);

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
    if (docId < mInMemBaseDocId)
    {
        mBitmap->Set(docId);
    }

    return mWriter.Delete(docId); // include in_mem_reader
}

inline bool DeletionMapReader::Delete(segmentid_t segId, docid_t localDocId)
{
    docid_t baseDocid = GetBaseDocId(segId);
    if (baseDocid == INVALID_DOCID)
    {
        return false;
    }
    return Delete(baseDocid + localDocId);
}

inline bool DeletionMapReader::IsDeleted(docid_t docId) const
{
    if (unlikely(docId >= (docid_t)mInMemBaseDocId))
    {
        return !mInMemDeletionMapReader ? true :
            mInMemDeletionMapReader->IsDeleted(docId - mInMemBaseDocId);
    }

    assert(mBitmap != NULL);
    return mBitmap->Test(docId);
}

inline bool DeletionMapReader::IsDeleted(
        segmentid_t segId, docid_t localDocId) const
{
    docid_t baseDocid = GetBaseDocId(segId);
    if (baseDocid == INVALID_DOCID)
    {
        return true;
    }
    return IsDeleted(baseDocid + localDocId);
}

inline uint32_t DeletionMapReader::GetDeletedDocCount() const
{ 
    uint32_t deleteCount = 0;
    if (mBitmap)
    {
        deleteCount += mBitmap->GetSetCount(); 
    }
    if (mInMemDeletionMapReader)
    {
        deleteCount += mInMemDeletionMapReader->GetDeletedDocCount();
    }
    return deleteCount;
}

inline docid_t DeletionMapReader::GetBaseDocId(segmentid_t segId) const
{
    if (mInMemDeletionMapReader&&
        unlikely(segId == mInMemDeletionMapReader->GetInMemSegmentId()))
    {
        return mInMemBaseDocId;
    }
    
    SegmentMetaMap::const_iterator iter = mSegmentMetaMap.find(segId);
    if (iter != mSegmentMetaMap.end())
    {
        const SegmentMeta& segMeta = iter->second;
        return segMeta.first;
    }
    return INVALID_DOCID;
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DELETION_MAP_READER_H
