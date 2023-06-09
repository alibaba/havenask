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
#ifndef __INDEXLIB_BLOCK_PRIMARY_KEY_PAIR_SEGMENT_ITERATOR_H
#define __INDEXLIB_BLOCK_PRIMARY_KEY_PAIR_SEGMENT_ITERATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/common/block_array/BlockArrayReader.h"
#include "indexlib/index/normal/primarykey/primary_key_pair_segment_iterator.h"
#include "indexlib/util/Status2Exception.h"
namespace indexlib { namespace index {

template <typename Key>
class BlockPrimaryKeyPairSegmentIterator : public PrimaryKeyPairSegmentIterator<Key>
{
public:
    typedef typename PrimaryKeyPairSegmentIterator<Key>::PKPair PKPair;
    BlockPrimaryKeyPairSegmentIterator() {}
    ~BlockPrimaryKeyPairSegmentIterator() {}

    BlockPrimaryKeyPairSegmentIterator(const BlockPrimaryKeyPairSegmentIterator&) = delete;
    BlockPrimaryKeyPairSegmentIterator& operator=(const BlockPrimaryKeyPairSegmentIterator&) = delete;
    BlockPrimaryKeyPairSegmentIterator(BlockPrimaryKeyPairSegmentIterator&&) = delete;
    BlockPrimaryKeyPairSegmentIterator& operator=(BlockPrimaryKeyPairSegmentIterator&&) = delete;

public:
    void Init(const file_system::FileReaderPtr& fileReader) override;
    bool HasNext() const override;
    void Next(PKPair& pkPair) override;
    void GetCurrentPKPair(PKPair& pair) const override;
    uint64_t GetPkCount() const override;

private:
    std::shared_ptr<indexlibv2::index::BlockArrayIterator<Key, docid_t>> mIterator;
    IE_LOG_DECLARE();
};

template <typename Key>
void BlockPrimaryKeyPairSegmentIterator<Key>::Init(const file_system::FileReaderPtr& fileReader)
{
    indexlibv2::index::BlockArrayReader<Key, docid_t> reader;
    reader.Init(fileReader, file_system::DirectoryPtr(), fileReader->GetLength(), false);
    auto [status, iter] = reader.CreateIterator();
    THROW_IF_STATUS_ERROR(status);
    mIterator.reset(iter);
}

template <typename Key>
bool BlockPrimaryKeyPairSegmentIterator<Key>::HasNext() const
{
    return mIterator->HasNext();
}

template <typename Key>
void BlockPrimaryKeyPairSegmentIterator<Key>::Next(PKPair& pkPair)
{
    auto status = mIterator->Next(&pkPair.key, &pkPair.docid);
    THROW_IF_STATUS_ERROR(status);
}

template <typename Key>
void BlockPrimaryKeyPairSegmentIterator<Key>::GetCurrentPKPair(PKPair& pair) const
{
    return mIterator->GetCurrentKVPair(&pair.key, &pair.docid);
}

template <typename Key>
uint64_t BlockPrimaryKeyPairSegmentIterator<Key>::GetPkCount() const
{
    return mIterator->GetKeyCount();
}
}} // namespace indexlib::index

#endif //__INDEXLIB_BLOCK_PRIMARY_KEY_PAIR_SEGMENT_ITERATOR_H
