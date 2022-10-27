
#ifndef __INDEXLIB_ATTRIBUTE_ITERATOR_TYPED_H
#define __INDEXLIB_ATTRIBUTE_ITERATOR_TYPED_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_base.h"
#include "indexlib/index/normal/attribute/accessor/building_attribute_reader.h"

IE_NAMESPACE_BEGIN(index);

template<typename T, typename ReaderTraits = AttributeReaderTraits<T> >
class AttributeIteratorTyped : public AttributeIteratorBase
{
public:
    typedef typename ReaderTraits::SegmentReader SegmentReader;
    typedef std::tr1::shared_ptr<SegmentReader> SegmentReaderPtr;
    typedef BuildingAttributeReader<T, ReaderTraits> BuildingAttributeReaderType;
    typedef std::tr1::shared_ptr<BuildingAttributeReaderType> BuildingAttributeReaderPtr;

public:
    AttributeIteratorTyped(const std::vector<SegmentReaderPtr>& segReaders,
                           const index_base::SegmentInfos& segInfos,
                           autil::mem_pool::Pool* pool);

    AttributeIteratorTyped(const std::vector<SegmentReaderPtr>& segReaders,
                           const BuildingAttributeReaderPtr& buildingAttributeReader,
                           const index_base::SegmentInfos& segInfos, docid_t buildingBaseDocId,
                           autil::mem_pool::Pool* pool);

    virtual ~AttributeIteratorTyped() {}

public:
    void Reset() override;

    template <typename ValueType>
    inline bool Seek(docid_t docId, ValueType& value) __ALWAYS_INLINE;
    
    inline const char* GetBaseAddress(docid_t docId) __ALWAYS_INLINE;

    inline bool UpdateValue(docid_t docId, const T &value);

private:
    template <typename ValueType>
    bool SeekInRandomMode(docid_t docId, ValueType& value);

protected:
    const std::vector<SegmentReaderPtr>& mSegmentReaders;
    const index_base::SegmentInfos& mSegmentInfos;
    BuildingAttributeReaderPtr mBuildingAttributeReader;
    docid_t mCurrentSegmentBaseDocId;
    docid_t mCurrentSegmentEndDocId;
    uint32_t mSegmentCursor;
    docid_t mBuildingBaseDocId;
    size_t mBuildingSegIdx;

private:
    friend class AttributeIteratorTypedTest;
    friend class VarNumAttributeReaderTest;
    IE_LOG_DECLARE();
};

template<typename T, typename ReaderTraits>
AttributeIteratorTyped<T, ReaderTraits>::AttributeIteratorTyped(
        const std::vector<SegmentReaderPtr>& segReaders,
        const index_base::SegmentInfos& segInfos,
        autil::mem_pool::Pool* pool)
    : AttributeIteratorBase(pool)
    , mSegmentReaders(segReaders)
    , mSegmentInfos(segInfos)
    , mBuildingBaseDocId(INVALID_DOCID)
{
    Reset();
}

template<typename T, typename ReaderTraits>
AttributeIteratorTyped<T, ReaderTraits>::AttributeIteratorTyped(
        const std::vector<SegmentReaderPtr>& segReaders,
        const BuildingAttributeReaderPtr& buildingAttributeReader,
        const index_base::SegmentInfos& segInfos,
        docid_t buildingBaseDocId, autil::mem_pool::Pool* pool)
    : AttributeIteratorBase(pool)
    , mSegmentReaders(segReaders)
    , mSegmentInfos(segInfos)
    , mBuildingAttributeReader(buildingAttributeReader)
    , mBuildingBaseDocId(buildingBaseDocId)
{
    Reset();
}


template<typename T, typename ReaderTraits>
void AttributeIteratorTyped<T, ReaderTraits>::Reset()
{
    mSegmentCursor = 0;
    mCurrentSegmentBaseDocId = 0;
    mBuildingSegIdx = 0;

    if (mSegmentInfos.size() > 0)
    {
        mCurrentSegmentEndDocId = mSegmentInfos[0].docCount;
    }
    else
    {
        mCurrentSegmentEndDocId = 0;
    }
}

template<typename T, typename ReaderTraits>
template<typename ValueType>
inline bool AttributeIteratorTyped<T, ReaderTraits>::Seek(docid_t docId, ValueType& value)
{
    if (docId >= mCurrentSegmentBaseDocId && docId < mCurrentSegmentEndDocId)
    {
        return mSegmentReaders[mSegmentCursor]->Read(docId - mCurrentSegmentBaseDocId, value, mPool);
    }

    if (docId >= mCurrentSegmentEndDocId)
    {
        mSegmentCursor++;
        for (; mSegmentCursor < mSegmentReaders.size(); mSegmentCursor++)
        {
            mCurrentSegmentBaseDocId = mCurrentSegmentEndDocId;
            mCurrentSegmentEndDocId += mSegmentInfos[mSegmentCursor].docCount;
            if (docId < mCurrentSegmentEndDocId)
            {
                docid_t localId = docId - mCurrentSegmentBaseDocId;
                return mSegmentReaders[mSegmentCursor]->Read(localId, value, mPool);
            }
        }
        mSegmentCursor = mSegmentReaders.size() - 1;
        return mBuildingAttributeReader &&
            mBuildingAttributeReader->Read(docId, value, mBuildingSegIdx, mPool);
    }

    Reset();
    return SeekInRandomMode(docId, value);
}

template<typename T, typename ReaderTraits>
inline const char* AttributeIteratorTyped<T, ReaderTraits>::GetBaseAddress(docid_t docId)
{
    // TODO
    return NULL;
}

template<typename T, typename ReaderTraits>
template<typename ValueType>
bool AttributeIteratorTyped<T, ReaderTraits>::SeekInRandomMode(docid_t docId, ValueType& value)
{
    docid_t baseDocId = 0;
    for (size_t i = 0; i < mSegmentInfos.size(); ++i)
    {
        if (docId < baseDocId + (docid_t)mSegmentInfos[i].docCount)
        {
            mSegmentCursor = i;
            mCurrentSegmentBaseDocId = baseDocId;
            mCurrentSegmentEndDocId = baseDocId + mSegmentInfos[i].docCount;
            return mSegmentReaders[i]->Read(docId - baseDocId, value, mPool);
        }
        baseDocId += mSegmentInfos[i].docCount;
    }

    mCurrentSegmentEndDocId = baseDocId;
    if (mSegmentInfos.size() > 0)
    {
        mSegmentCursor = mSegmentInfos.size() - 1;
        mCurrentSegmentBaseDocId = 
            mCurrentSegmentEndDocId - mSegmentInfos[mSegmentCursor].docCount;
    }
    else
    {
        mSegmentCursor = 0;
        mCurrentSegmentBaseDocId = 0;
    }
    return mBuildingAttributeReader &&
        mBuildingAttributeReader->Read(docId, value, mBuildingSegIdx, mPool);
}

template <typename T, typename ReaderTraits>
inline bool AttributeIteratorTyped<T, ReaderTraits>::UpdateValue(
        docid_t docId, const T &value)
{
    docid_t baseDocId = 0;
    for (size_t i = 0; i < mSegmentInfos.size(); i++) 
    {
        if (docId < baseDocId + (docid_t)mSegmentInfos[i].docCount)
        {
            return mSegmentReaders[i]->UpdateField(docId - baseDocId, 
                    (uint8_t *)&value, sizeof(T));
        }
        baseDocId += mSegmentInfos[i].docCount;
    }
    return mBuildingAttributeReader && mBuildingAttributeReader->UpdateField(
            docId, (uint8_t *)&value, sizeof(T));
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTE_ITERATOR_TYPED_H
