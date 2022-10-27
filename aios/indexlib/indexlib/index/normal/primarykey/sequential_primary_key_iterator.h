#ifndef __INDEXLIB_SEQUENTIAL_PRIMARY_KEY_ITERATOR_H
#define __INDEXLIB_SEQUENTIAL_PRIMARY_KEY_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/misc/exception.h"
#include "indexlib/index/normal/primarykey/primary_key_iterator.h"
#include "indexlib/index/normal/primarykey/sorted_primary_key_pair_segment_iterator.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/config/index_config.h"
#include "indexlib/config/primary_key_index_config.h"

IE_NAMESPACE_BEGIN(index);

template <typename Key>
class SequentialPrimaryKeyIterator : public PrimaryKeyIterator<Key>
{
private:
    typedef PKPair<Key> TypedPKPair;
    typedef std::tr1::shared_ptr<PrimaryKeyPairSegmentIterator<Key> > SegmentIteratorPtr;
public:
    SequentialPrimaryKeyIterator(const config::IndexConfigPtr& indexConfig)
        : PrimaryKeyIterator<Key>(indexConfig)
        , mCurrentIteratorIdx(0)
        , mTotalPkCount(0)
        , mTotalDocCount(0)
    {}
    
    ~SequentialPrimaryKeyIterator()
    {}
    
public:
    void Init(const std::vector<index_base::SegmentData>& segmentDatas) override;
    uint64_t GetPkCount() const override;
    uint64_t GetDocCount() const override;
    bool HasNext() const override;
    void Next(TypedPKPair& pkPair) override;

private:
    SegmentIteratorPtr CreateSegmentIterator(
        const file_system::FileReaderPtr &fileReader) const;
private:
    std::vector<SegmentIteratorPtr> mSegmentIterators;
    std::vector<docid_t> mBaseDocIds;
    size_t mCurrentIteratorIdx;
    uint64_t mTotalPkCount;
    uint64_t mTotalDocCount;
private:
    IE_LOG_DECLARE();
};

template <typename Key>
void SequentialPrimaryKeyIterator<Key>::Init(
    const std::vector<index_base::SegmentData>& segmentDatas)
{
    mCurrentIteratorIdx = 0;
    mTotalPkCount = 0;
    mTotalDocCount = 0;
    for (size_t i = 0; i < segmentDatas.size(); i++)
    {
        uint32_t docCount = segmentDatas[i].GetSegmentInfo().docCount;
        if (docCount == 0)
        {
            continue;
        }
        file_system::FileReaderPtr fileReader =
            PrimaryKeyIterator<Key>::OpenPKDataFile(segmentDatas[i].GetDirectory());
        SegmentIteratorPtr segIterator = CreateSegmentIterator(fileReader);
        mSegmentIterators.push_back(segIterator);
        mBaseDocIds.push_back(segmentDatas[i].GetBaseDocId());
        mTotalDocCount += docCount;
        mTotalPkCount += segIterator->GetPkCount();
    }
}

template <typename Key>
uint64_t SequentialPrimaryKeyIterator<Key>::GetPkCount() const
{
    return mTotalPkCount;
}

template <typename Key>
uint64_t SequentialPrimaryKeyIterator<Key>::GetDocCount() const
{
    return mTotalDocCount;
}

template <typename Key>
bool SequentialPrimaryKeyIterator<Key>::HasNext() const
{
    size_t iteratorCount = mSegmentIterators.size();
    if (iteratorCount == 0)
    {
        return false;
    }
    if (mCurrentIteratorIdx != iteratorCount - 1)
    {
        return true;
    }
    return mSegmentIterators[mCurrentIteratorIdx]->HasNext();
}

template <typename Key>
void SequentialPrimaryKeyIterator<Key>::Next(TypedPKPair& pkPair)
{
    assert(HasNext());
    if (!mSegmentIterators[mCurrentIteratorIdx]->HasNext())
    {
        ++mCurrentIteratorIdx;
    }
    mSegmentIterators[mCurrentIteratorIdx]->Next(pkPair);
    pkPair.docid += mBaseDocIds[mCurrentIteratorIdx];
}

template <typename Key>
typename SequentialPrimaryKeyIterator<Key>::SegmentIteratorPtr
SequentialPrimaryKeyIterator<Key>::CreateSegmentIterator(
    const file_system::FileReaderPtr &fileReader) const
{
    config::PrimaryKeyIndexConfigPtr primaryKeyIndexConfig = 
        DYNAMIC_POINTER_CAST(config::PrimaryKeyIndexConfig, this->mIndexConfig);
    assert(primaryKeyIndexConfig);

    SegmentIteratorPtr segmentIterator;
    if (primaryKeyIndexConfig->GetPrimaryKeyIndexType() == pk_sort_array)
    {
        segmentIterator.reset(new SortedPrimaryKeyPairSegmentIterator<Key>);
    }
    else
    {
        // todo: support hash_table
        INDEXLIB_THROW(misc::UnSupportedException,
                       "SequentialPrimaryKey does not support pk_hash_table");
    }
    assert(segmentIterator);
    segmentIterator->Init(fileReader);
    return segmentIterator;
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SEQUENTIAL_PRIMARY_KEY_ITERATOR_H
