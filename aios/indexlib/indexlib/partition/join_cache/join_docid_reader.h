#ifndef __INDEXLIB_JOIN_DOCID_READER_H
#define __INDEXLIB_JOIN_DOCID_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader_typed.h"
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_typed.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/partition/join_cache/join_docid_reader_base.h"

IE_NAMESPACE_BEGIN(partition);

template<typename JoinAttrType, typename PKIndexType>
class JoinDocidReader : public JoinDocidReaderBase
{
public:
    typedef index::AttributeIteratorTyped<JoinAttrType> JoinAttributeIterator;
    typedef index::AttributeIteratorTyped<docid_t> JoinCacheIterator;
    typedef index::PrimaryKeyIndexReaderTyped<PKIndexType> PrimaryKeyIndexReader;
    typedef std::tr1::shared_ptr<index::PrimaryKeyIndexReader> PrimaryKeyIndexReaderPtr;
public:
    JoinDocidReader(std::string joinAttrName, JoinCacheIterator* joinCacheIter,
                    JoinAttributeIterator* mainAttributeIter,
                    index::PrimaryKeyIndexReaderPtr auxPkReader,
                    index::DeletionMapReaderPtr auxDelMapReader);
    ~JoinDocidReader();
public:
    
    docid_t GetAuxDocid(docid_t mainDocid);
private:
    std::string mJoinAttrName;
    JoinCacheIterator* mJoinCacheIter;
    JoinAttributeIterator* mMainAttributeIter;
    index::PrimaryKeyIndexReaderPtr mAuxPkReader;
    index::DeletionMapReaderPtr mAuxDelMapReader;
    IE_LOG_DECLARE();
};


template<typename JoinAttrType, typename PKIndexType>
JoinDocidReader<JoinAttrType, PKIndexType>::JoinDocidReader(
    std::string joinAttrName, JoinCacheIterator* joinCacheIter,
    JoinAttributeIterator* mainAttributeIter,
    index::PrimaryKeyIndexReaderPtr auxPkReader,
    index::DeletionMapReaderPtr auxDelMapReader)
    : mJoinAttrName(joinAttrName)
    , mJoinCacheIter(joinCacheIter)
    , mMainAttributeIter(mainAttributeIter)
    , mAuxPkReader(auxPkReader)
    , mAuxDelMapReader(auxDelMapReader)
{
}

template<typename JoinAttrType, typename PKIndexType>
JoinDocidReader<JoinAttrType, PKIndexType>::~JoinDocidReader() 
{
    if (mJoinCacheIter)
    {
        POOL_DELETE_CLASS(mJoinCacheIter);
    }
    if (mMainAttributeIter)
    {
        POOL_DELETE_CLASS(mMainAttributeIter);
    }
}

template<typename JoinAttrType, typename PKIndexType>
inline docid_t JoinDocidReader<JoinAttrType, PKIndexType>::GetAuxDocid(docid_t mainDocid)
{
    docid_t joinDocId = INVALID_DOCID;
    if (mJoinCacheIter)
    {
        if (mJoinCacheIter->Seek(mainDocid, joinDocId)
            && joinDocId != INVALID_DOCID
            && !mAuxDelMapReader->IsDeleted(joinDocId))
        {
            return joinDocId;
        }
    }

    JoinAttrType joinAttrValue;
    if (mMainAttributeIter->Seek(mainDocid, joinAttrValue))
    {
        const std::string &strValue = autil::StringUtil::toString(joinAttrValue);
        joinDocId = mAuxPkReader->Lookup(strValue);
        if (mJoinCacheIter)
        {
            mJoinCacheIter->UpdateValue(mainDocid, joinDocId);
        }
        return joinDocId;
    }
    return INVALID_DOCID;
}

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_JOIN_DOCID_READER_H
