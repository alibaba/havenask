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

#include "autil/Log.h"
#include "indexlib/base/Status.h"
#include "indexlib/index/primary_key/PrimaryKeyHashTable.h"
#include "indexlib/index/primary_key/PrimaryKeyLeafIterator.h"

namespace indexlibv2::index {
template <typename Key>
class HashTablePrimaryKeyLeafIterator : public PrimaryKeyLeafIterator<Key>
{
public:
    HashTablePrimaryKeyLeafIterator() : _pkCounts(0), _pkCursor(0), _docIdInCurSeg(-1), _isDone(false) {}
    ~HashTablePrimaryKeyLeafIterator() {}

public:
    using PKPairTyped = PKPair<Key, docid_t>;

public:
    Status Init(const indexlib::file_system::FileReaderPtr& fileReader) override;
    bool HasNext() const override;
    Status Next(PKPairTyped& pkPair) override;
    void GetCurrentPKPair(PKPairTyped& pair) const override;
    uint64_t GetPkCount() const override { return _pkCounts; }

private:
    indexlib::file_system::FileReaderPtr _fileReader;
    uint64_t _pkCounts;
    uint64_t _pkCursor;
    docid_t _docIdInCurSeg;
    bool _isDone;
    PKPairTyped _pkCurrentPair;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, HashTablePrimaryKeyLeafIterator, T);

template <typename Key>
Status HashTablePrimaryKeyLeafIterator<Key>::Init(const indexlib::file_system::FileReaderPtr& fileReader)
{
    _fileReader = fileReader;
    _isDone = false;
    if (PrimaryKeyHashTable<Key>::SeekToPkPair(_fileReader, _pkCounts)) {
        return Next(_pkCurrentPair);
    }
    AUTIL_LOG(ERROR, "Fail to get pkCounts in HashTablePrimaryKeyLeafIterator");
    return Status::IOError("Fail to get pkCounts in HashTablePrimaryKeyLeafIterator");
}

template <typename Key>
bool HashTablePrimaryKeyLeafIterator<Key>::HasNext() const
{
    return !_isDone;
}

template <typename Key>
Status HashTablePrimaryKeyLeafIterator<Key>::Next(PKPairTyped& pkPair)
{
    GetCurrentPKPair(pkPair);
    if (_pkCursor >= _pkCounts) {
        _docIdInCurSeg = -1;
        _isDone = true;
        return Status::OK();
    }
    do {
        size_t readSize = _fileReader->Read((void*)&_pkCurrentPair, sizeof(PKPairTyped)).GetOrThrow();
        ++_docIdInCurSeg;
        (void)readSize;
        if (readSize != sizeof(PKPairTyped)) {
            AUTIL_LOG(ERROR, "Fail to read value from HashTablePrimaryKeyLeafIterator");
            return Status::IOError("Fail to read value from HashTablePrimaryKeyLeafIterator");
        }
    } while (PrimaryKeyHashTable<Key>::IsInvalidPkPair(_pkCurrentPair));
    ++_pkCursor;
    _pkCurrentPair.docid = _docIdInCurSeg;
    return Status::OK();
}

template <typename Key>
void HashTablePrimaryKeyLeafIterator<Key>::GetCurrentPKPair(PKPairTyped& pair) const
{
    pair = _pkCurrentPair;
}

} // namespace indexlibv2::index
