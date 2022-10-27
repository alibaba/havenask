#ifndef __INDEXLIB_COMBINE_SEGMENTS_PRIMARY_KEY_ITERATOR_H
#define __INDEXLIB_COMBINE_SEGMENTS_PRIMARY_KEY_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/primarykey/primary_key_iterator.h"
#include "indexlib/index/normal/primarykey/primary_key_hash_table.h"
#include "indexlib/index/normal/primarykey/primary_key_iterator_creator.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/config/index_config.h"

IE_NAMESPACE_BEGIN(index);

template <typename Key>
class CombineSegmentsPrimaryKeyIterator : public PrimaryKeyIterator<Key>
{
private:
    typedef PKPair<Key> TypedPKPair;
    typedef std::tr1::shared_ptr<PrimaryKeyIterator<Key> > PrimaryKeyIteratorPtr;
public:
    CombineSegmentsPrimaryKeyIterator(const config::PrimaryKeyIndexConfigPtr& pkConfig);
    ~CombineSegmentsPrimaryKeyIterator();
public:
    void Init(const std::vector<index_base::SegmentData>& segmentDatas) override;
    uint64_t GetPkCount() const override;
    uint64_t GetDocCount() const override;
    bool HasNext() const override;
    void Next(TypedPKPair& pkPair) override;

private:
    PrimaryKeyIteratorPtr mPrimaryKeyIterator;
    docid_t mBaseDocId;
private:
    IE_LOG_DECLARE();
};

template <typename Key>
CombineSegmentsPrimaryKeyIterator<Key>::CombineSegmentsPrimaryKeyIterator(
        const config::PrimaryKeyIndexConfigPtr& pkConfig)
    : PrimaryKeyIterator<Key>(pkConfig)
    , mBaseDocId(0)
{
    mPrimaryKeyIterator = PrimaryKeyIteratorCreator::Create<Key>(pkConfig);
}

template <typename Key>
CombineSegmentsPrimaryKeyIterator<Key>::~CombineSegmentsPrimaryKeyIterator()
{
}

template<typename Key>
void CombineSegmentsPrimaryKeyIterator<Key>::Init(
        const std::vector<index_base::SegmentData>& segmentDatas)
{
    mPrimaryKeyIterator->Init(segmentDatas);
    if (segmentDatas.size() > 0)
    {
        mBaseDocId = segmentDatas[0].GetBaseDocId();
    }
    else
    {
        mBaseDocId = 0;
    }
}

template<typename Key>
uint64_t CombineSegmentsPrimaryKeyIterator<Key>::GetPkCount() const
{
    return mPrimaryKeyIterator->GetPkCount();
}

template<typename Key>
uint64_t CombineSegmentsPrimaryKeyIterator<Key>::GetDocCount() const
{
    return mPrimaryKeyIterator->GetDocCount();
}

template<typename Key>
bool CombineSegmentsPrimaryKeyIterator<Key>::HasNext() const
{
    return mPrimaryKeyIterator->HasNext();
}

template<typename Key>
void CombineSegmentsPrimaryKeyIterator<Key>::Next(TypedPKPair& pkPair)
{
    mPrimaryKeyIterator->Next(pkPair);
    pkPair.docid -= mBaseDocId;
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_COMBINE_SEGMENTS_PRIMARY_KEY_ITERATOR_H
