/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <memory>
#include <queue>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/primarykey/ordered_primary_key_iterator.h"
#include "indexlib/index/normal/primarykey/primary_key_loader.h"
#include "indexlib/index/normal/primarykey/primary_key_pair.h"
#include "indexlib/index/normal/primarykey/primary_key_segment_reader_typed.h"
#include "indexlib/index/normal/primarykey/sorted_primary_key_pair_segment_iterator.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename Key>
class OnDiskOrderedPrimaryKeyIterator : public OrderedPrimaryKeyIterator<Key>
{
public:
    typedef typename PrimaryKeyPairSegmentIterator<Key>::PKPair PKPair;

private:
    typedef PrimaryKeySegmentReaderTyped<Key> SegmentReader;
    typedef std::shared_ptr<SegmentReader> SegmentReaderPtr;
    typedef std::pair<docid_t, SegmentReaderPtr> PrimaryKeySegment;
    typedef std::vector<PrimaryKeySegment> PKSegmentList;

    typedef std::shared_ptr<PrimaryKeyPairSegmentIterator<Key>> SegmentIteratorPtr;
    typedef std::pair<SegmentIteratorPtr, docid_t> SegmentIteratorPair;
    typedef std::vector<SegmentIteratorPair> SegmentIteratorPairVec;

private:
    class SegmentIteratorComp
    {
    public:
        // lft.first = SegmentIterator lft.second = segment baseDocid
        bool operator()(const SegmentIteratorPair& lft, const SegmentIteratorPair& rht)
        {
            PKPair leftPkPair;
            lft.first->GetCurrentPKPair(leftPkPair);

            PKPair rightPkPair;
            rht.first->GetCurrentPKPair(rightPkPair);

            if (leftPkPair.key == rightPkPair.key) {
                assert(lft.second != rht.second);
                return lft.second > rht.second;
            }
            return leftPkPair.key > rightPkPair.key;
        }
    };
    typedef std::priority_queue<SegmentIteratorPair, SegmentIteratorPairVec, SegmentIteratorComp> Heap;

public:
    OnDiskOrderedPrimaryKeyIterator(const config::IndexConfigPtr& indexConfig)
        : OrderedPrimaryKeyIterator<Key>(indexConfig)
    {
    }

    ~OnDiskOrderedPrimaryKeyIterator() {}

public:
    void Init(const std::vector<index_base::SegmentData>& segmentDatas) override;

    void Init(const PKSegmentList& segmentList);

    bool HasNext() const override { return !mHeap.empty(); }
    void Next(PKPair& pair) override;

private:
    Heap mHeap;
    PKSegmentList mSegmentList;

private:
    IE_LOG_DECLARE();
};

///////////////////////////////////////////////////////////////////
template <typename Key>
void OnDiskOrderedPrimaryKeyIterator<Key>::Init(const PKSegmentList& segmentList)
{
    // hold segment reader memory
    mSegmentList = segmentList;
    for (typename PKSegmentList::const_iterator iter = segmentList.begin(); iter != segmentList.end(); ++iter) {
        const SegmentReaderPtr& segmentReader = iter->second;
        docid_t baseDocid = iter->first;
        mHeap.push(std::make_pair(segmentReader->CreateIterator(), baseDocid));
    }
}

template <typename Key>
void OnDiskOrderedPrimaryKeyIterator<Key>::Init(const std::vector<index_base::SegmentData>& segmentDatas)
{
    config::PrimaryKeyIndexConfigPtr primaryKeyIndexConfig =
        DYNAMIC_POINTER_CAST(config::PrimaryKeyIndexConfig, this->mIndexConfig);
    assert(primaryKeyIndexConfig);

    for (size_t i = 0; i < segmentDatas.size(); i++) {
        const std::shared_ptr<const index_base::SegmentInfo>& segInfo = segmentDatas[i].GetSegmentInfo();
        if (segInfo->docCount == 0) {
            continue;
        }
        SegmentIteratorPtr iterator;
        switch (primaryKeyIndexConfig->GetPrimaryKeyIndexType()) {
        case pk_sort_array:
            iterator.reset(new SortedPrimaryKeyPairSegmentIterator<Key>);
            break;
        case pk_block_array:
            iterator.reset(new BlockPrimaryKeyPairSegmentIterator<Key>);
            break;
        default:
            INDEXLIB_THROW(util::UnSupportedException, "not support pk type [%d]",
                           primaryKeyIndexConfig->GetPrimaryKeyIndexType());
        }
        file_system::DirectoryPtr pkDir =
            PrimaryKeyLoader<Key>::GetPrimaryKeyDirectory(segmentDatas[i].GetDirectory(), primaryKeyIndexConfig);
        file_system::FileReaderPtr fileReader =
            PrimaryKeyLoader<Key>::OpenPKDataFile(pkDir, primaryKeyIndexConfig->GetPrimaryKeyIndexType());

        iterator->Init(fileReader);
        mHeap.push(std::make_pair(iterator, segmentDatas[i].GetBaseDocId()));
    }
}

template <typename Key>
void OnDiskOrderedPrimaryKeyIterator<Key>::Next(PKPair& pair)
{
    SegmentIteratorPair iterPair = mHeap.top();
    mHeap.pop();
    SegmentIteratorPtr& segmentIter = iterPair.first;
    segmentIter->Next(pair);
    pair.docid += iterPair.second;
    if (segmentIter->HasNext()) {
        mHeap.push(iterPair);
    }
}
}} // namespace indexlib::index
