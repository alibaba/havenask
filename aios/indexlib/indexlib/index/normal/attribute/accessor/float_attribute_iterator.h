#ifndef __INDEXLIB_ATTRIBUTE_ITERATOR_TYPED_H
#define __INDEXLIB_ATTRIBUTE_ITERATOR_TYPED_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_base.h"
#include "indexlib/index/normal/attribute/accessor/building_attribute_reader.h"

IE_NAMESPACE_BEGIN(index);

template<>
class AttributeIteratorTyped<float> : public AttributeIteratorBase
{
public:
    typedef AttributeReaderTraits<float>::SegmentReader SegmentReader;
    typedef AttributeReaderTraits<int16_t>::SegmentReader Fp16SegmentReader;
    typedef AttributeReaderTraits<int8_t>::SegmentReader Fp8SegmentReader;
    
    typedef std::tr1::shared_ptr<SegmentReader> SegmentReaderPtr;
    typedef std::tr1::shared_ptr<Fp16SegmentReader> Fp16SegmentReaderPtr;
    typedef std::tr1::shared_ptr<Fp8SegmentReader> Fp8SegmentReaderPtr;
    
    typedef BuildingAttributeReader<float, AttributeReaderTraits<float>> BuildingAttributeReaderType;
    typedef std::tr1::shared_ptr<BuildingAttributeReaderType> BuildingAttributeReaderPtr;

public:
    AttributeIteratorTyped<float>(const std::vector<SegmentReaderPtr>& segReaders,
                                  const index_base::SegmentInfos& segInfos,
                                  autil::mem_pool::Pool* pool);

    AttributeIteratorTyped<float>(config::CompressTypeOption compressType,
                                  const std::vector<SegmentReaderPtr>& segReaders,
                                  const std::vector<Fp16SegmentReaderPtr>& fp16SegReaders,
                                  const std::vector<Fp8SegmentReaderPtr>& fp8SegReaders,
                                  const index_base::SegmentInfos& segInfos,
                                  autil::mem_pool::Pool* pool);

    AttributeIteratorTyped<float>(const std::vector<SegmentReaderPtr>& segReaders,
                           const BuildingAttributeReaderPtr& buildingAttributeReader,
                           const index_base::SegmentInfos& segInfos, docid_t buildingBaseDocId,
                           autil::mem_pool::Pool* pool);

    virtual ~AttributeIteratorTyped<float>() {}
public:
    void Reset() override;

    template <typename ValueType>
    inline bool Seek(docid_t docId, ValueType& value) __ALWAYS_INLINE;

    inline const char* GetBaseAddress(docid_t docId) __ALWAYS_INLINE;

    inline bool UpdateValue(docid_t docId, const T &value);

private:
    template <typename ValueType>
    bool SeekInRandomMode(docid_t docId, ValueType& value);

    template <typename ValueType, typename ActualSegmentReader>
    inline bool InnerSeek(docid_t docId, ValueType& value,
                          ActualSegmentReader& segReaders) __ALWAYS_INLINE;


protected:
    static const std::vector<SegmentReaderPtr> EMPTY_SEG_READER;
    static const std::vector<Fp16SegmentReaderPtr> EMPTY_FP16_READER;
    static const std::vector<Fp8SegmentReaderPtr> EMPTY_FP8_READER;

    const std::vector<SegmentReaderPtr>& mSegmentReaders;
    const std::vector<Fp16SegmentReaderPtr>& mFp16SegmentReaders;
    const std::vector<Fp8SegmentReaderPtr>& mFp8SegmentReaders;
    const index_base::SegmentInfos& mSegmentInfos;
    BuildingAttributeReaderPtr mBuildingAttributeReader;
    docid_t mCurrentSegmentBaseDocId;
    docid_t mCurrentSegmentEndDocId;
    uint32_t mSegmentCursor;
    docid_t mBuildingBaseDocId;
    size_t mBuildingSegIdx;
    config::CompressTypeOption mCompressType;

private:
    friend class AttributeIteratorTypedTest;
    friend class VarNumAttributeReaderTest;
    IE_LOG_DECLARE();
};

template<>
AttributeIteratorTyped<float>::AttributeIteratorTyped(
        const std::vector<SegmentReaderPtr>& segReaders,
        const index_base::SegmentInfos& segInfos,
        autil::mem_pool::Pool* pool)
    : AttributeIteratorBase(pool)
    , mSegmentReaders(segReaders)
    , mFp16SegmentReaders(EMPTY_FP16_READER)
    , mFp8SegmentReaders(EMPTY_FP8_READER)
    , mSegmentInfos(segInfos)
    , mBuildingBaseDocId(INVALID_DOCID)
{
    Reset();
}

template<>
AttributeIteratorTyped<float>::AttributeIteratorTyped(
        const std::vector<SegmentReaderPtr>& segReaders,
        const index_base::SegmentInfos& segInfos,
        autil::mem_pool::Pool* pool)
    : AttributeIteratorBase(pool)
    , mSegmentReaders(segReaders)
    , mFp16SegmentReaders(EMPTY_FP16_READER)
    , mFp8SegmentReaders(EMPTY_FP8_READER)
    , mSegmentInfos(segInfos)
    , mBuildingBaseDocId(INVALID_DOCID)
{
    Reset();
}

template<>
AttributeIteratorTyped::AttributeIteratorTyped<float>(
    config::CompressTypeOption compressType,
    const std::vector<SegmentReaderPtr>& segReaders,
    const std::vector<Fp16SegmentReaderPtr>& fp16SegReaders,
    const std::vector<Fp8SegmentReaderPtr>& fp8SegReaders,
    const index_base::SegmentInfos& segInfos,
    autil::mem_pool::Pool* pool)
    : AttributeIteratorBase(pool)
    , mSegmentReaders(segReaders)
    , mFp16SegmentReaders(fp16SegReaders)
    , mFp8SegmentReaders(fp8SegReaders)
    , mSegmentInfos(segInfos)
    , mBuildingBaseDocId(INVALID_DOCID)
    , mCompressType(compressType)
{ 
}

template<>
template<typename ValueType>
inline bool AttributeIteratorTyped<float>::Seek(docid_t docId, ValueType& value)
{
    if (mCompressType.HasFp16EncodeCompress()) {
        return InnerSeek(docId, value, mFp16SegmentReaders);
    } else if (mCompressType.HasInt8EncodeCompress()) {
        return InnerSeek(docId, value, mFp8SegmentReaders);
    }
    return InnerSeek(docId, value, mSegmentReaders);
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTE_ITERATOR_TYPED_H
