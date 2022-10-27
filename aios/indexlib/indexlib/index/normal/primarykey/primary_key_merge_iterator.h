#ifndef __INDEXLIB_PRIMARY_KEY_MERGE_ITERATOR_H
#define __INDEXLIB_PRIMARY_KEY_MERGE_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/primarykey/ordered_primary_key_iterator.h"
#include "indexlib/index/normal/primarykey/hash_primary_key_iterator.h"
#include "indexlib/index/normal/primarykey/on_disk_ordered_primary_key_iterator.h"
#include "indexlib/index/normal/reclaim_map/reclaim_map.h"

IE_NAMESPACE_BEGIN(index);

template <typename Key>
class PrimaryKeyMergeIterator : public OrderedPrimaryKeyIterator<Key>
{
public:
    typedef PKPair<Key> TypedPKPair;
    typedef OnDiskOrderedPrimaryKeyIterator<Key> KeyOrderedPrimaryKeyIterator;
    typedef HashPrimaryKeyIterator<Key> TypedHashPrimaryKeyIterator;
    typedef std::tr1::shared_ptr<PrimaryKeyIterator<Key> > PrimaryKeyIteratorPtr;
    
public:
    PrimaryKeyMergeIterator(const config::PrimaryKeyIndexConfigPtr& pkConfig,
                            const ReclaimMapPtr& reclaimMap);
    ~PrimaryKeyMergeIterator() {}
public:
    uint64_t GetPkCount() const override
    { return mReclaimMap->GetNewDocCount(); }
    uint64_t GetDocCount() const override
    { return mReclaimMap->GetNewDocCount(); }
    
    void Init(const std::vector<index_base::SegmentData>& segmentDatas) override;
    bool HasNext() const override;
    void Next(TypedPKPair& pair) override;
private:
    TypedPKPair GetNextValidPkPair();
private:
    PrimaryKeyIteratorPtr mOrderedIter;
    ReclaimMapPtr mReclaimMap;
    TypedPKPair mCurrentPkPair;
    
    static TypedPKPair INVALID_PK_PAIR;
private:
    IE_LOG_DECLARE();
};

template<typename Key>
PKPair<Key> PrimaryKeyMergeIterator<Key>::INVALID_PK_PAIR = { Key(), INVALID_DOCID };

template<typename Key>
 PrimaryKeyMergeIterator<Key>::PrimaryKeyMergeIterator(
     const config::PrimaryKeyIndexConfigPtr& pkConfig,
     const ReclaimMapPtr& reclaimMap)
  : OrderedPrimaryKeyIterator<Key>(pkConfig)
  , mReclaimMap(reclaimMap)
{
    assert(pkConfig);
    assert(mReclaimMap);
    if (pkConfig->GetPrimaryKeyIndexType() == pk_hash_table)
    {
        // TODO: use docid ordered iterator
        mOrderedIter.reset(new TypedHashPrimaryKeyIterator(pkConfig));
    }
    else
    {
        mOrderedIter.reset(new KeyOrderedPrimaryKeyIterator(pkConfig));
    }
    mCurrentPkPair.key = Key();
    mCurrentPkPair.docid = INVALID_DOCID;
}

template<typename Key>
void PrimaryKeyMergeIterator<Key>::Init(const std::vector<index_base::SegmentData>& segmentDatas)
{
    mOrderedIter->Init(segmentDatas);
    mCurrentPkPair = GetNextValidPkPair();
}

template<typename Key>
bool PrimaryKeyMergeIterator<Key>::HasNext() const
{
    return !(mCurrentPkPair.docid == INVALID_DOCID);
}

template<typename Key>
void PrimaryKeyMergeIterator<Key>::Next(TypedPKPair& pair)
{
    pair = mCurrentPkPair;
    mCurrentPkPair = GetNextValidPkPair();

    while (mCurrentPkPair.docid != INVALID_DOCID && mCurrentPkPair.key == pair.key)
    {
        IE_LOG(ERROR, "primary key duplicated");
        pair = mCurrentPkPair;
        mCurrentPkPair = GetNextValidPkPair();
    }
}

template<typename Key>
PKPair<Key> PrimaryKeyMergeIterator<Key>::GetNextValidPkPair()
{
    TypedPKPair pkPair = INVALID_PK_PAIR;
    while (mOrderedIter->HasNext())
    {
        mOrderedIter->Next(pkPair);
        docid_t newDocId = mReclaimMap->GetNewId(pkPair.docid);
        if (newDocId != INVALID_DOCID)
        {
            pkPair.docid = newDocId;
            return pkPair;
        }
    }
    return INVALID_PK_PAIR;
}

IE_LOG_SETUP_TEMPLATE(index, PrimaryKeyMergeIterator);
IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PRIMARY_KEY_MERGE_ITERATOR_H
