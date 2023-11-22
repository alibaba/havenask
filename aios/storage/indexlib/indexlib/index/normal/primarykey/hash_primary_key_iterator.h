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

#include "indexlib/common_define.h"
#include "indexlib/config/index_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/normal/primarykey/primary_key_hash_table.h"
#include "indexlib/index/normal/primarykey/primary_key_iterator.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename Key>
class HashPrimaryKeyIterator : public PrimaryKeyIterator<Key>
{
private:
    typedef PKPair<Key> TypedPKPair;

public:
    HashPrimaryKeyIterator(const config::IndexConfigPtr& indexConfig)
        : PrimaryKeyIterator<Key>(indexConfig)
        , mTotalPkCount(0)
        , mTotalDocCount(0)
        , mCurrentReaderIdx(0)
        , mCurrentPkCount(0)
        , mDocIdInCurSeg(-1)
    {
    }

    ~HashPrimaryKeyIterator() {}

public:
    void Init(const std::vector<index_base::SegmentData>& segmentDatas) override;
    uint64_t GetPkCount() const override;
    uint64_t GetDocCount() const override;
    bool HasNext() const override;
    void Next(TypedPKPair& pkPair) override;

private:
    void Reset();

private:
    std::vector<file_system::FileReaderPtr> mReaders;
    std::vector<uint64_t> mPkCounts;
    std::vector<docid_t> mBaseDocIds;
    uint64_t mTotalPkCount;
    uint64_t mTotalDocCount;

    size_t mCurrentReaderIdx;
    uint64_t mCurrentPkCount;
    docid_t mDocIdInCurSeg;

private:
    IE_LOG_DECLARE();
};

template <typename Key>
void HashPrimaryKeyIterator<Key>::Init(const std::vector<index_base::SegmentData>& segmentDatas)
{
    Reset();
    for (size_t i = 0; i < segmentDatas.size(); i++) {
        uint32_t docCount = segmentDatas[i].GetSegmentInfo()->docCount;
        if (docCount == 0) {
            continue;
        }
        file_system::FileReaderPtr fileReader = PrimaryKeyIterator<Key>::OpenPKDataFile(segmentDatas[i].GetDirectory());
        uint64_t pkCount = PrimaryKeyHashTable<Key>::SeekToPkPair(fileReader);
        mTotalPkCount += pkCount;
        mPkCounts.push_back(pkCount);
        mReaders.push_back(fileReader);
        mBaseDocIds.push_back(segmentDatas[i].GetBaseDocId());
        mTotalDocCount += docCount;
    }
}

template <typename Key>
uint64_t HashPrimaryKeyIterator<Key>::GetPkCount() const
{
    return mTotalPkCount;
}

template <typename Key>
uint64_t HashPrimaryKeyIterator<Key>::GetDocCount() const
{
    return mTotalDocCount;
}

template <typename Key>
bool HashPrimaryKeyIterator<Key>::HasNext() const
{
    size_t readerCount = mReaders.size();
    if (readerCount == 0) {
        return false;
    }
    if (mCurrentReaderIdx != readerCount - 1) {
        return true;
    }
    if (mCurrentPkCount < mPkCounts[mCurrentReaderIdx]) {
        return true;
    }
    return false;
}

template <typename Key>
void HashPrimaryKeyIterator<Key>::Next(TypedPKPair& pkPair)
{
    assert(HasNext());
    if (mCurrentPkCount == mPkCounts[mCurrentReaderIdx]) {
        ++mCurrentReaderIdx;
        mCurrentPkCount = 0;
        mDocIdInCurSeg = -1;
    }
    assert(mCurrentReaderIdx < mReaders.size());
    file_system::FileReaderPtr& fileReader = mReaders[mCurrentReaderIdx];
    do {
        size_t readSize = fileReader->Read((void*)&pkPair, sizeof(TypedPKPair)).GetOrThrow();
        (void)readSize;
        assert(readSize == sizeof(TypedPKPair));
        ++mDocIdInCurSeg;
    } while (PrimaryKeyHashTable<Key>::IsInvalidPkPair(pkPair));
    pkPair.docid = mBaseDocIds[mCurrentReaderIdx] + mDocIdInCurSeg;
    ++mCurrentPkCount;
}

template <typename Key>
void HashPrimaryKeyIterator<Key>::Reset()
{
    mTotalPkCount = 0;
    mTotalDocCount = 0;
    mCurrentReaderIdx = 0;
    mCurrentPkCount = 0;
    mDocIdInCurSeg = -1;

    mReaders.clear();
    mPkCounts.clear();
    mBaseDocIds.clear();
}
}} // namespace indexlib::index
