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
#ifndef __INDEXLIB_SEQUENTIAL_PRIMARY_KEY_ITERATOR_H
#define __INDEXLIB_SEQUENTIAL_PRIMARY_KEY_ITERATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_config.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/normal/primarykey/block_primary_key_pair_segment_iterator.h"
#include "indexlib/index/normal/primarykey/primary_key_iterator.h"
#include "indexlib/index/normal/primarykey/sorted_primary_key_pair_segment_iterator.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Exception.h"

namespace indexlib { namespace index {

template <typename Key>
class SequentialPrimaryKeyIterator : public PrimaryKeyIterator<Key>
{
private:
    typedef PKPair<Key> TypedPKPair;
    typedef std::shared_ptr<PrimaryKeyPairSegmentIterator<Key>> SegmentIteratorPtr;

public:
    SequentialPrimaryKeyIterator(const config::IndexConfigPtr& indexConfig)
        : PrimaryKeyIterator<Key>(indexConfig)
        , mCurrentIteratorIdx(0)
        , mTotalPkCount(0)
        , mTotalDocCount(0)
    {
    }

    ~SequentialPrimaryKeyIterator() {}

public:
    void Init(const std::vector<index_base::SegmentData>& segmentDatas) override;
    uint64_t GetPkCount() const override;
    uint64_t GetDocCount() const override;
    bool HasNext() const override;
    void Next(TypedPKPair& pkPair) override;

private:
    SegmentIteratorPtr CreateSegmentIterator(const file_system::FileReaderPtr& fileReader) const;

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
void SequentialPrimaryKeyIterator<Key>::Init(const std::vector<index_base::SegmentData>& segmentDatas)
{
    mCurrentIteratorIdx = 0;
    mTotalPkCount = 0;
    mTotalDocCount = 0;
    for (size_t i = 0; i < segmentDatas.size(); i++) {
        uint32_t docCount = segmentDatas[i].GetSegmentInfo()->docCount;
        if (docCount == 0) {
            continue;
        }
        file_system::FileReaderPtr fileReader = PrimaryKeyIterator<Key>::OpenPKDataFile(segmentDatas[i].GetDirectory());
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
    if (iteratorCount == 0) {
        return false;
    }
    if (mCurrentIteratorIdx != iteratorCount - 1) {
        return true;
    }
    return mSegmentIterators[mCurrentIteratorIdx]->HasNext();
}

template <typename Key>
void SequentialPrimaryKeyIterator<Key>::Next(TypedPKPair& pkPair)
{
    assert(HasNext());
    if (!mSegmentIterators[mCurrentIteratorIdx]->HasNext()) {
        ++mCurrentIteratorIdx;
    }
    mSegmentIterators[mCurrentIteratorIdx]->Next(pkPair);
    pkPair.docid += mBaseDocIds[mCurrentIteratorIdx];
}

template <typename Key>
typename SequentialPrimaryKeyIterator<Key>::SegmentIteratorPtr
SequentialPrimaryKeyIterator<Key>::CreateSegmentIterator(const file_system::FileReaderPtr& fileReader) const
{
    config::PrimaryKeyIndexConfigPtr primaryKeyIndexConfig =
        DYNAMIC_POINTER_CAST(config::PrimaryKeyIndexConfig, this->mIndexConfig);
    assert(primaryKeyIndexConfig);

    SegmentIteratorPtr segmentIterator;
    switch (primaryKeyIndexConfig->GetPrimaryKeyIndexType()) {
    case pk_sort_array:
        segmentIterator.reset(new SortedPrimaryKeyPairSegmentIterator<Key>);
        break;
    case pk_block_array:
        segmentIterator.reset(new BlockPrimaryKeyPairSegmentIterator<Key>);
        break;
    default:
        // todo: support hash_table
        INDEXLIB_THROW(util::UnSupportedException, "SequentialPrimaryKey does not support pk_hash_table");
    }
    assert(segmentIterator);
    segmentIterator->Init(fileReader);
    return segmentIterator;
}
}} // namespace indexlib::index

#endif //__INDEXLIB_SEQUENTIAL_PRIMARY_KEY_ITERATOR_H
