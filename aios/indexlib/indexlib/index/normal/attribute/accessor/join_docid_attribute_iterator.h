#ifndef __INDEXLIB_JOIN_DOCID_ATTRIBUTE_ITERATOR_H
#define __INDEXLIB_JOIN_DOCID_ATTRIBUTE_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_typed.h"

IE_NAMESPACE_BEGIN(index);

class JoinDocidAttributeIterator : public AttributeIteratorTyped<docid_t>
{
public:
    JoinDocidAttributeIterator(const std::vector<SegmentReaderPtr>& segReaders,
                               const BuildingAttributeReaderPtr& buildingAttributeReader,
                               const index_base::SegmentInfos& segInfos,
                               docid_t buildingBaseDocId,
                               const std::vector<docid_t>& joinedBaseDocIds,
                               const std::vector<docid_t>& joinedBuildingBaseDocIds,
                               autil::mem_pool::Pool* pool)
        : AttributeIteratorTyped<docid_t>(segReaders, buildingAttributeReader, 
                segInfos, buildingBaseDocId, pool)
        , mJoinedBaseDocIds(joinedBaseDocIds)
        , mJoinedBuildingBaseDocIds(joinedBuildingBaseDocIds)
    {}

    ~JoinDocidAttributeIterator() {}

public:
    inline bool Seek(docid_t docId, docid_t& value) __ALWAYS_INLINE;

private:
    std::vector<docid_t> mJoinedBaseDocIds;
    std::vector<docid_t> mJoinedBuildingBaseDocIds;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(JoinDocidAttributeIterator);

/////////////////////////////////////////////////////////////
inline bool JoinDocidAttributeIterator::Seek(docid_t docId, docid_t& value)
{
    //TODO:test
    if (!AttributeIteratorTyped<docid_t>::Seek(docId, value))
    {
        return false;
    }

    if (!mBuildingAttributeReader || docId < mBuildingBaseDocId)
    {
        assert(!mJoinedBaseDocIds.empty());
        assert(mJoinedBaseDocIds.size() == mSegmentInfos.size());
        assert(mSegmentCursor < mSegmentInfos.size());
        value += mJoinedBaseDocIds[mSegmentCursor];
    }
    else
    {
        value += mJoinedBuildingBaseDocIds[mBuildingSegIdx];
    }
    return true;
}


IE_NAMESPACE_END(index);

#endif //__INDEXLIB_JOIN_DOCID_ATTRIBUTE_ITERATOR_H
