#ifndef __INDEXLIB_ON_DISK_ORDERED_PRIMARY_KEY_ITERATOR_H
#define __INDEXLIB_ON_DISK_ORDERED_PRIMARY_KEY_ITERATOR_H

#include <tr1/memory>
#include <queue>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/primarykey/primary_key_pair.h"
#include "indexlib/index/normal/primarykey/primary_key_segment_reader_typed.h"
#include "indexlib/index/normal/primarykey/sorted_primary_key_pair_segment_iterator.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index/normal/primarykey/primary_key_loader.h"
#include "indexlib/index/normal/primarykey/ordered_primary_key_iterator.h"

IE_NAMESPACE_BEGIN(index);

template<typename Key>
class OnDiskOrderedPrimaryKeyIterator : public OrderedPrimaryKeyIterator<Key>
{
public:
    typedef typename PrimaryKeyPairSegmentIterator<Key>::PKPair PKPair;
    
private:
    typedef PrimaryKeySegmentReaderTyped<Key> SegmentReader;
    typedef std::tr1::shared_ptr<SegmentReader> SegmentReaderPtr;
    typedef std::pair<docid_t, SegmentReaderPtr> PrimaryKeySegment;
    typedef std::vector<PrimaryKeySegment> PKSegmentList;

    typedef std::tr1::shared_ptr<PrimaryKeyPairSegmentIterator<Key> > SegmentIteratorPtr;
    typedef std::pair<SegmentIteratorPtr, docid_t> SegmentIteratorPair;
    typedef std::vector<SegmentIteratorPair> SegmentIteratorPairVec;

private:
    class SegmentIteratorComp
    {
    public:
        //lft.first = SegmentIterator lft.second = segment baseDocid
        bool operator() (const SegmentIteratorPair& lft, const SegmentIteratorPair& rht)
        {
            PKPair leftPkPair;
            lft.first->GetCurrentPKPair(leftPkPair);

            PKPair rightPkPair;
            rht.first->GetCurrentPKPair(rightPkPair);

            if (leftPkPair.key == rightPkPair.key)
            {
                assert(lft.second != rht.second);
                return lft.second > rht.second;
            }
            return leftPkPair.key > rightPkPair.key;
        }
    };
    typedef std::priority_queue<SegmentIteratorPair, SegmentIteratorPairVec,
                                SegmentIteratorComp> Heap;

public:
    OnDiskOrderedPrimaryKeyIterator(const config::IndexConfigPtr& indexConfig)
        : OrderedPrimaryKeyIterator<Key>(indexConfig)
    {}
    
    ~OnDiskOrderedPrimaryKeyIterator() {}
    
public:
    void Init(const std::vector<index_base::SegmentData>& segmentDatas);

    void Init(const PKSegmentList& segmentList);

    bool HasNext() const { return !mHeap.empty(); }
    void Next(PKPair& pair);

private:
    Heap mHeap;
    PKSegmentList mSegmentList;

private:
    IE_LOG_DECLARE();
};

///////////////////////////////////////////////////////////////////
template<typename Key>
void OnDiskOrderedPrimaryKeyIterator<Key>::Init(const PKSegmentList& segmentList)
{
    // hold segment reader memory
    mSegmentList = segmentList;
    for (typename PKSegmentList::const_iterator iter = segmentList.begin();
         iter != segmentList.end(); ++iter)
    {
        const SegmentReaderPtr& segmentReader = iter->second;
        docid_t baseDocid = iter->first;
        mHeap.push(std::make_pair(segmentReader->CreateIterator(), baseDocid));
    }
}

template<typename Key>
void OnDiskOrderedPrimaryKeyIterator<Key>::Init(
    const std::vector<index_base::SegmentData>& segmentDatas)
{
    config::PrimaryKeyIndexConfigPtr primaryKeyIndexConfig = 
        DYNAMIC_POINTER_CAST(config::PrimaryKeyIndexConfig, this->mIndexConfig);
    assert(primaryKeyIndexConfig);

    for (size_t i = 0; i < segmentDatas.size(); i++)
    {
        const index_base::SegmentInfo& segInfo = segmentDatas[i].GetSegmentInfo();
        if (segInfo.docCount == 0)
        {
            continue;
        }
        SegmentIteratorPtr iterator;
        if (primaryKeyIndexConfig->GetPrimaryKeyIndexType() == pk_sort_array)
        {
            iterator.reset(new SortedPrimaryKeyPairSegmentIterator<Key>);
        }
        else
        {
            INDEXLIB_THROW(misc::UnSupportedException, "not support pk type [%d]", 
                    primaryKeyIndexConfig->GetPrimaryKeyIndexType());
        }
        file_system::DirectoryPtr pkDir = 
            PrimaryKeyLoader<Key>::GetPrimaryKeyDirectory(
                segmentDatas[i].GetDirectory(), primaryKeyIndexConfig);
        file_system::FileReaderPtr fileReader =
            PrimaryKeyLoader<Key>::OpenPKDataFile(pkDir);

        iterator->Init(fileReader);
        mHeap.push(std::make_pair(iterator, segmentDatas[i].GetBaseDocId()));
    }
}

template<typename Key>
void OnDiskOrderedPrimaryKeyIterator<Key>::Next(PKPair& pair)
{
    SegmentIteratorPair iterPair = mHeap.top();
    mHeap.pop();
    SegmentIteratorPtr& segmentIter = iterPair.first;
    segmentIter->Next(pair);
    pair.docid += iterPair.second;
    if (segmentIter->HasNext())
    {
        mHeap.push(iterPair);
    }
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ON_DISK_ORDERED_PRIMARY_KEY_ITERATOR_H
