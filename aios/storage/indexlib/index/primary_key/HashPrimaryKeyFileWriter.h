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

#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/primary_key/PrimaryKeyFileWriter.h"
#include "indexlib/index/primary_key/PrimaryKeyHashTable.h"
#include "indexlib/util/PrimeNumberTable.h"

namespace indexlibv2 { namespace index {

template <typename Key>
class HashPrimaryKeyFileWriter : public PrimaryKeyFileWriter<Key>
{
public:
    typedef PKPair<Key, docid_t> PrimaryKeyPair;
    typedef indexlib::util::HashMap<Key, docid_t> HashMapType;
    typedef std::shared_ptr<HashMapType> HashMapTypePtr;

public:
    HashPrimaryKeyFileWriter() {}
    ~HashPrimaryKeyFileWriter() {}

public:
    void Init(size_t docCount, size_t pkCount, const indexlib::file_system::FileWriterPtr& file,
              autil::mem_pool::PoolBase* pool) override;

    Status AddPKPair(Key key, docid_t docid) override;
    Status AddSortedPKPair(Key key, docid_t docid) override
    {
        assert(false);
        return AddPKPair(key, docid);
    }

    Status Close() override;

    int64_t EstimateDumpTempMemoryUse(size_t docCount) override
    {
        return docCount * PrimaryKeyHashTable<Key>::EstimateMemoryCostPerDoc();
    }

private:
    PrimaryKeyHashTable<Key> _pkHashTable;
    char* _buffer = nullptr;
    size_t _bufLen = 0;
    indexlib::file_system::FileWriterPtr _file;
    autil::mem_pool::PoolBase* _pool = nullptr;

private:
    AUTIL_LOG_DECLARE();
    friend class HashPrimaryKeyFileWriterTest;
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, HashPrimaryKeyFileWriter, T);

template <typename Key>
void HashPrimaryKeyFileWriter<Key>::Init(size_t docCount, size_t pkCount,
                                         const indexlib::file_system::FileWriterPtr& file,
                                         autil::mem_pool::PoolBase* pool)
{
    _bufLen = PrimaryKeyHashTable<Key>::CalculateMemorySize(pkCount, docCount);
    _buffer = IE_POOL_COMPATIBLE_NEW_VECTOR(pool, char, _bufLen);
    _pkHashTable.Init(_buffer, pkCount, docCount);
    _pool = pool;
    _file = file;
}

template <typename Key>
Status HashPrimaryKeyFileWriter<Key>::AddPKPair(Key key, docid_t docid)
{
    PrimaryKeyPair pkPair;
    pkPair.key = key;
    pkPair.docid = docid;
    _pkHashTable.Insert(pkPair);
    return Status::OK();
}

template <typename Key>
Status HashPrimaryKeyFileWriter<Key>::Close()
{
    // TODO: try catch??
    _file->ReserveFile(_bufLen).GetOrThrow();
    auto [status, writeSize] = _file->Write(_buffer, _bufLen).StatusWith();
    if (!status.IsOK()) {
        return status;
    }
    assert(_file->GetLength() == _bufLen);
    IE_POOL_COMPATIBLE_DELETE_VECTOR(_pool, _buffer, _bufLen);
    return _file->Close().Status();
}

}} // namespace indexlibv2::index
