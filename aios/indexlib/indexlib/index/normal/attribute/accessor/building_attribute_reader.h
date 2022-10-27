#ifndef __INDEXLIB_BUILDING_ATTRIBUTE_READER_H
#define __INDEXLIB_BUILDING_ATTRIBUTE_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_traits.h"

IE_NAMESPACE_BEGIN(index);

template<typename T, typename ReaderTraits = AttributeReaderTraits<T> >
class BuildingAttributeReader
{
public:
    typedef typename ReaderTraits::InMemSegmentReader InMemSegmentReader;
    typedef std::tr1::shared_ptr<InMemSegmentReader> InMemSegmentReaderPtr;

public:
    BuildingAttributeReader() {}
    ~BuildingAttributeReader() {}
    
public:
    void AddSegmentReader(docid_t baseDocId,
                          const InMemSegmentReaderPtr& inMemSegReader)
    {
        if (inMemSegReader)
        {
            mBaseDocIds.push_back(baseDocId);
            mSegmentReaders.push_back(inMemSegReader);
        }
    }

    template <typename ValueType>
    inline bool Read(docid_t docId, ValueType& value, size_t& idx,
                     autil::mem_pool::Pool* pool = NULL) const __ALWAYS_INLINE;
    
    inline bool Read(docid_t docId, uint8_t* buf,
                     uint32_t bufLen, size_t& idx) __ALWAYS_INLINE;
    inline bool UpdateField(docid_t docId, uint8_t* buf, uint32_t bufLen);

    docid_t GetBaseDocId(size_t idx) const
    { return idx < mBaseDocIds.size() ? mBaseDocIds[idx] : INVALID_DOCID; }

    size_t Size() const { return mBaseDocIds.size(); }
    
private:
    std::vector<docid_t> mBaseDocIds;
    std::vector<InMemSegmentReaderPtr> mSegmentReaders;

private:
    IE_LOG_DECLARE();
};

///////////////////////////////////////////////////////////////////////////
template<typename T, typename ReaderTraits>
template <typename ValueType>
inline bool BuildingAttributeReader<T, ReaderTraits>::Read(
        docid_t docId, ValueType& value, size_t& idx, autil::mem_pool::Pool* pool) const
{
    for (size_t i = 0; i < mSegmentReaders.size(); i++)
    {
        docid_t curBaseDocId = mBaseDocIds[i];
        if (docId < curBaseDocId)
        {
            return false;
        }
        if (mSegmentReaders[i]->Read(docId - curBaseDocId, value, pool))
        {
            idx = i;
            return true;
        }
    }
    return false;
}

template<typename T, typename ReaderTraits>
inline bool BuildingAttributeReader<T, ReaderTraits>::Read(
        docid_t docId, uint8_t* buf, uint32_t bufLen, size_t& idx)
{
    for (size_t i = 0; i < mSegmentReaders.size(); i++)
    {
        docid_t curBaseDocId = mBaseDocIds[i];
        if (docId < curBaseDocId)
        {
            return false;
        }
        if (mSegmentReaders[i]->Read(docId - curBaseDocId, buf, bufLen))
        {
            idx = i;
            return true;
        }
    }
    return false;
}

template<typename T, typename ReaderTraits>
inline bool BuildingAttributeReader<T, ReaderTraits>::UpdateField(
        docid_t docId, uint8_t* buf, uint32_t bufLen)
{
    for (size_t i = 0; i < mSegmentReaders.size(); i++)
    {
        docid_t curBaseDocId = mBaseDocIds[i];
        if (docId < curBaseDocId)
        {
            return false;
        }
        if (mSegmentReaders[i]->UpdateField(docId - curBaseDocId, buf, bufLen))
        {
            return true;
        }
    }
    return false;
}


IE_NAMESPACE_END(index);

#endif //__INDEXLIB_BUILDING_ATTRIBUTE_READER_H
