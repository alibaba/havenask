#ifndef __INDEXLIB_DOC_FILTER_PROCESSOR_H
#define __INDEXLIB_DOC_FILTER_PROCESSOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/diversity_constrain.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_meta_reader.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_attribute_reader.h"

DECLARE_REFERENCE_CLASS(index, PostingIterator);

IE_NAMESPACE_BEGIN(index);

class DocFilterProcessor
{
public:
    DocFilterProcessor() {}
    virtual ~DocFilterProcessor() {}

public:
    virtual bool BeginFilter(dictkey_t key, 
                             const PostingIteratorPtr& postingIt) = 0;
    virtual bool IsFiltered(docid_t docId) = 0;
    virtual void SetTruncateMetaReader(
            const TruncateMetaReaderPtr &metaReader) = 0;
};

template <typename T>
class DocFilterProcessorTyped : public DocFilterProcessor
{
public:
    DocFilterProcessorTyped(const TruncateAttributeReaderPtr attrReader,
                            const config::DiversityConstrain& constrain);
    ~DocFilterProcessorTyped();

public:
    bool BeginFilter(dictkey_t key, const PostingIteratorPtr& postingIt);
    bool IsFiltered(docid_t docId);
    void SetTruncateMetaReader(const TruncateMetaReaderPtr &metaReader);
    void SetPostingIterator(const PostingIteratorPtr& postingIt){};

    // for test
    void GetFilterRange(int64_t &min, int64_t &max);

private:
    TruncateAttributeReaderPtr mAttrReader;
    TruncateMetaReaderPtr mMetaReader;
    int64_t mMinValue;
    int64_t mMaxValue;
    uint64_t mMask;
};

template <typename T>
DocFilterProcessorTyped<T>::DocFilterProcessorTyped(
        const TruncateAttributeReaderPtr attrReader,
        const config::DiversityConstrain& constrain)
{
    mMaxValue = constrain.GetFilterMaxValue();
    mMinValue = constrain.GetFilterMinValue();
    mMask = constrain.GetFilterMask();
    mAttrReader = attrReader;
}

template<typename T>
DocFilterProcessorTyped<T>::~DocFilterProcessorTyped()
{
}

template<typename T>
inline bool DocFilterProcessorTyped<T>::BeginFilter(dictkey_t key, const PostingIteratorPtr& postingIt)
{
    if (mMetaReader && !mMetaReader->Lookup(key, mMinValue, mMaxValue))
    {
        return false;
    }
    return true;
}

template<typename T>
inline bool DocFilterProcessorTyped<T>::IsFiltered(docid_t docId)
{
    T value;
    mAttrReader->Read(docId, (uint8_t *)&value, sizeof(T));
    value &= (T)mMask;
    return !(mMinValue <= (int64_t)value && (int64_t)value <= mMaxValue);
}

template<typename T>
inline void DocFilterProcessorTyped<T>::SetTruncateMetaReader(
        const TruncateMetaReaderPtr &metaReader)
{
    mMetaReader = metaReader;
}

template<>
inline bool DocFilterProcessorTyped<autil::uint128_t>::IsFiltered(docid_t docId)
{
    assert(false);
    return false;
}

template<>
inline bool DocFilterProcessorTyped<float>::IsFiltered(docid_t docId)
{
    assert(false);
    return false;
}

template<>
inline bool DocFilterProcessorTyped<double>::IsFiltered(docid_t docId)
{
    assert(false);
    return false;
}

template<typename T>
inline void DocFilterProcessorTyped<T>::GetFilterRange(int64_t &min, int64_t &max)
{
    min = mMinValue;
    max = mMaxValue;
}

DEFINE_SHARED_PTR(DocFilterProcessor);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DOC_FILTER_PROCESSOR_H
