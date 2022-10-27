#ifndef __INDEXLIB_JOIN_DOCID_ATTRIBUTE_READER_H
#define __INDEXLIB_JOIN_DOCID_ATTRIBUTE_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/join_docid_attribute_iterator.h"

IE_NAMESPACE_BEGIN(index);

class JoinDocidAttributeReader : public SingleValueAttributeReader<docid_t>
{
public:
    JoinDocidAttributeReader();
    virtual ~JoinDocidAttributeReader();
public:
    void InitJoinBaseDocId(const index_base::PartitionDataPtr& partitionData);

    AttributeReader* Clone() const override;

    JoinDocidAttributeIterator* CreateIterator(
            autil::mem_pool::Pool *pool) const override;

    bool Read(docid_t docId, std::string& attrValue,
              autil::mem_pool::Pool* pool = NULL) const override;

public:
    docid_t GetJoinDocId(docid_t docId) const;

protected:
    std::vector<docid_t> mJoinedBaseDocIds;
    std::vector<docid_t> mJoinedBuildingBaseDocIds;

private:
    IE_LOG_DECLARE();
};

inline JoinDocidAttributeIterator* JoinDocidAttributeReader::CreateIterator(
        autil::mem_pool::Pool *pool) const
{
    return IE_POOL_COMPATIBLE_NEW_CLASS(pool, JoinDocidAttributeIterator, 
            mSegmentReaders, mBuildingAttributeReader, 
            mSegmentInfos, mBuildingBaseDocId, 
            mJoinedBaseDocIds, mJoinedBuildingBaseDocIds, pool);
}

inline docid_t JoinDocidAttributeReader::GetJoinDocId(docid_t docId) const
{
    docid_t joinDocId = INVALID_DOCID;
    docid_t baseDocId = 0;
    assert(mJoinedBaseDocIds.size() == mSegmentInfos.size());
    for (size_t i = 0; i < mSegmentInfos.size(); i++)
    {
        docid_t docCount = (docid_t)mSegmentInfos[i].docCount;
        if (docId < baseDocId + docCount)
        {
            if (!mSegmentReaders[i]->Read(docId - baseDocId, joinDocId))
            {
                return INVALID_DOCID;
            }
            joinDocId += mJoinedBaseDocIds[i];
            return joinDocId;
        }
        baseDocId += docCount;
    }
    
    if (mBuildingAttributeReader)
    {
        size_t buildingSegIdx = 0;
        if (mBuildingAttributeReader->Read(docId, joinDocId, buildingSegIdx))
        {
            return joinDocId + mJoinedBuildingBaseDocIds[buildingSegIdx];
        }
    }
    return INVALID_DOCID;
}

DEFINE_SHARED_PTR(JoinDocidAttributeReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_JOIN_DOCID_ATTRIBUTE_READER_H
