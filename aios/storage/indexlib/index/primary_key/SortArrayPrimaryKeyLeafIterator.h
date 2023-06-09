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

namespace indexlibv2::index {

template <typename Key>
class SortArrayPrimaryKeyLeafIterator : public PrimaryKeyLeafIterator<Key>
{
public:
    SortArrayPrimaryKeyLeafIterator() : _currentPKPair {0, INVALID_DOCID}, _length(0), _cursor(0), _isDone(true) {}
    ~SortArrayPrimaryKeyLeafIterator() {}

public:
    using PKPairTyped = PKPair<Key, docid_t>;

public:
    Status Init(const indexlib::file_system::FileReaderPtr& fileReader) override;
    bool HasNext() const override;
    Status Next(PKPairTyped& pkPair) override;
    void GetCurrentPKPair(PKPairTyped& pair) const override;
    uint64_t GetPkCount() const override;

private:
    indexlib::file_system::FileReaderPtr _fileReader;
    PKPairTyped _currentPKPair;
    size_t _length;
    size_t _cursor;
    bool _isDone;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, SortArrayPrimaryKeyLeafIterator, T);

template <typename Key>
Status SortArrayPrimaryKeyLeafIterator<Key>::Init(const indexlib::file_system::FileReaderPtr& fileReader)
{
    if (nullptr == fileReader) {
        AUTIL_LOG(ERROR, "file reader is nullptr");
        return Status::Unknown("fileReader is nullptr");
    }
    _fileReader = fileReader;
    _length = fileReader->GetLength();
    _cursor = 0;
    _isDone = false;
    return Next(_currentPKPair);
}

template <typename Key>
bool SortArrayPrimaryKeyLeafIterator<Key>::HasNext() const
{
    return !_isDone;
}

template <typename Key>
Status SortArrayPrimaryKeyLeafIterator<Key>::Next(PKPairTyped& pkPair)
{
    GetCurrentPKPair(pkPair);

    if (_cursor >= _length) {
        _isDone = true;
        return Status::OK();
    }

    auto [st, readLen] = _fileReader->Read((void*)(&_currentPKPair), sizeof(PKPairTyped), _cursor).StatusWith();
    RETURN_IF_STATUS_ERROR(st, "failed to read pk data file[%s], file collapse!", _fileReader->DebugString().c_str());
    _cursor += readLen;
    return Status::OK();
}

template <typename Key>
void SortArrayPrimaryKeyLeafIterator<Key>::GetCurrentPKPair(PKPairTyped& pair) const
{
    pair = _currentPKPair;
}

template <typename Key>
uint64_t SortArrayPrimaryKeyLeafIterator<Key>::GetPkCount() const
{
    return _fileReader->GetLength() / sizeof(PKPairTyped);
}

} // namespace indexlibv2::index
