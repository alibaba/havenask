#ifndef __INDEXLIB_SINGLE_VALUE_DATA_ITERATOR_H
#define __INDEXLIB_SINGLE_VALUE_DATA_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/attribute_value_type_traits.h"
#include "indexlib/index/normal/attribute/accessor/single_attribute_segment_reader_for_merge.h"
#include "indexlib/index/normal/attribute/accessor/attribute_data_iterator.h"

IE_NAMESPACE_BEGIN(index);

template <typename T>
class SingleValueDataIterator : public AttributeDataIterator
{
public:
    SingleValueDataIterator(const config::AttributeConfigPtr& attrConfig)
        : AttributeDataIterator(attrConfig)
        , mValue(T())
    {}
    
    ~SingleValueDataIterator() {}
    
public:
    bool Init(const index_base::PartitionDataPtr& partData,
              segmentid_t segId) override;
    void MoveToNext() override;
    std::string GetValueStr() const override;
    autil::ConstString GetValueBinaryStr(autil::mem_pool::Pool *pool) const override;
    autil::ConstString GetRawIndexImageValue() const override;
    T GetValue() const;
    
private:
    typedef index::SingleAttributeSegmentReaderForMerge<T> SegmentReader;
    typedef std::tr1::shared_ptr<SegmentReader> SegmentReaderPtr;
    
    SegmentReaderPtr mSegReader;
    T mValue;
    
private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, SingleValueDataIterator);
//////////////////////////////////////////////////////////////////////
template <typename T>
inline bool SingleValueDataIterator<T>::Init(
        const index_base::PartitionDataPtr& partData, segmentid_t segId)
{
    index_base::SegmentData segData = partData->GetSegmentData(segId);
    mDocCount = segData.GetSegmentInfo().docCount;
    if (mDocCount <= 0)
    {
        IE_LOG(INFO, "segment [%d] is empty, will not open segment reader.", segId);
        return true;
    }

    IE_LOG(INFO, "Init dfs segment reader for single attribute [%s]",
           mAttrConfig->GetAttrName().c_str());
    mSegReader.reset(new SegmentReader(mAttrConfig));
    mSegReader->Open(partData, segData.GetSegmentInfo(), segId);
    mCurDocId = 0;
    if (!mSegReader->Read(mCurDocId, (uint8_t*)&mValue, sizeof(T)))
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed,
                             "read value for doc [%d] failed!", mCurDocId);
        return false;
    }
    return true;
}

template <typename T>
inline void SingleValueDataIterator<T>::MoveToNext()
{
    ++mCurDocId;
    if (mCurDocId >= mDocCount)
    {
        IE_LOG(INFO, "reach eof!");
        return;
    }
    
    if (!mSegReader->Read(mCurDocId, (uint8_t*)&mValue, sizeof(T)))
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed,
                             "read value for doc [%d] failed!", mCurDocId);
    }
}

template <typename T>
inline T SingleValueDataIterator<T>::GetValue() const
{
    return mValue;
}

template <typename T>
inline std::string SingleValueDataIterator<T>::GetValueStr() const
{
    T value = GetValue();
    return index::AttributeValueTypeToString<T>::ToString(value);
}

template <typename T>
inline autil::ConstString SingleValueDataIterator<T>::GetRawIndexImageValue() const
{
    return autil::ConstString((const char*)&mValue, sizeof(T));
}

template <typename T>
inline autil::ConstString SingleValueDataIterator<T>::GetValueBinaryStr(
        autil::mem_pool::Pool *pool) const
{
    char* copyBuf = (char*)pool->allocate(sizeof(T));
    *(T*)copyBuf = mValue;
    return autil::ConstString((const char *)copyBuf, sizeof(T));
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SINGLE_VALUE_DATA_ITERATOR_H
