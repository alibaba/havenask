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

#include "indexlib/index/common/block_array/BlockArrayReader.h"
#include "indexlib/index/primary_key/PrimaryKeyLeafIterator.h"

namespace indexlibv2 { namespace index {

template <typename Key>
class BlockArrayPrimaryKeyLeafIterator : public PrimaryKeyLeafIterator<Key>
{
public:
    BlockArrayPrimaryKeyLeafIterator() {}
    ~BlockArrayPrimaryKeyLeafIterator() {}

public:
    using PKPairTyped = PKPair<Key, docid_t>;

public:
    Status Init(const indexlib::file_system::FileReaderPtr& fileReader) override;
    bool HasNext() const override;
    Status Next(PKPairTyped& pkPair) override;
    void GetCurrentPKPair(PKPairTyped& pair) const override;
    uint64_t GetPkCount() const override;

private:
    std::shared_ptr<BlockArrayIterator<Key, docid_t>> _iterator;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, BlockArrayPrimaryKeyLeafIterator, T);

template <typename Key>
Status BlockArrayPrimaryKeyLeafIterator<Key>::Init(const indexlib::file_system::FileReaderPtr& fileReader)
{
    indexlibv2::index::BlockArrayReader<Key, docid_t> reader;
    auto [status, ret] = reader.Init(fileReader, indexlib::file_system::DirectoryPtr(), fileReader->GetLength(), false);
    RETURN_IF_STATUS_ERROR(status, "block array reader init fail");
    auto [status1, iter] = reader.CreateIterator();
    RETURN_IF_STATUS_ERROR(status1, "create iterator fail");
    _iterator.reset(iter);
    return Status::OK();
}

template <typename Key>
bool BlockArrayPrimaryKeyLeafIterator<Key>::HasNext() const
{
    return _iterator->HasNext();
}

template <typename Key>
Status BlockArrayPrimaryKeyLeafIterator<Key>::Next(PKPairTyped& pkPair)
{
    return _iterator->Next(&pkPair.key, &pkPair.docid);
}

template <typename Key>
void BlockArrayPrimaryKeyLeafIterator<Key>::GetCurrentPKPair(PKPairTyped& pair) const
{
    return _iterator->GetCurrentKVPair(&pair.key, &pair.docid);
}

template <typename Key>
uint64_t BlockArrayPrimaryKeyLeafIterator<Key>::GetPkCount() const
{
    return _iterator->GetKeyCount();
}
}} // namespace indexlibv2::index
