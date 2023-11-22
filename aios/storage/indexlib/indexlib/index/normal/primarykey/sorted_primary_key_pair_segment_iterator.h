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
#include "indexlib/index/normal/primarykey/primary_key_pair_segment_iterator.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename Key>
class SortedPrimaryKeyPairSegmentIterator : public PrimaryKeyPairSegmentIterator<Key>
{
public:
    typedef typename PrimaryKeyPairSegmentIterator<Key>::PKPair PKPair;

public:
    SortedPrimaryKeyPairSegmentIterator() : mLength(0), mCursor(0), mIsDone(true) {}

    virtual ~SortedPrimaryKeyPairSegmentIterator() {}

public:
    void Init(const file_system::FileReaderPtr& fileReader) override;
    bool HasNext() const override;
    void Next(PKPair& pkPair) override;
    void GetCurrentPKPair(PKPair& pair) const override;
    uint64_t GetPkCount() const override;

private:
    file_system::FileReaderPtr mFileReader;
    PKPair mCurrentPKPair;
    size_t mLength;
    size_t mCursor;
    bool mIsDone;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, SortedPrimaryKeyPairSegmentIterator);

//////////////////////////////////////////////////////////////////////
template <typename Key>
void SortedPrimaryKeyPairSegmentIterator<Key>::Init(const file_system::FileReaderPtr& fileReader)
{
    assert(fileReader);
    mFileReader = fileReader;
    mLength = fileReader->GetLength();
    mCursor = 0;
    mIsDone = false;
    Next(mCurrentPKPair);
}

template <typename Key>
bool SortedPrimaryKeyPairSegmentIterator<Key>::HasNext() const
{
    return !mIsDone;
}

template <typename Key>
void SortedPrimaryKeyPairSegmentIterator<Key>::Next(PKPair& pkPair)
{
    assert(HasNext());
    GetCurrentPKPair(pkPair);

    if (mCursor >= mLength) {
        mIsDone = true;
        return;
    }

    size_t readLen = mFileReader->Read((void*)(&mCurrentPKPair), sizeof(PKPair), mCursor).GetOrThrow();
    if (readLen != sizeof(PKPair)) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "read pk data file[%s] fail, file collapse!",
                             mFileReader->DebugString().c_str());
    }
    mCursor += readLen;
}

template <typename Key>
void SortedPrimaryKeyPairSegmentIterator<Key>::GetCurrentPKPair(PKPair& pair) const
{
    pair = mCurrentPKPair;
}

template <typename Key>
uint64_t SortedPrimaryKeyPairSegmentIterator<Key>::GetPkCount() const
{
    return mFileReader->GetLength() / sizeof(PKPair);
}
}} // namespace indexlib::index
