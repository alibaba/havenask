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

#include <cmath>
#include <memory>

#include "autil/Log.h"
#include "indexlib/base/Constant.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/primary_key/PrimaryKeyPair.h"
#include "indexlib/util/HashUtil.h"
#include "indexlib/util/PrimeNumberTable.h"

namespace indexlibv2 { namespace index {

template <typename Key>
class PrimaryKeyHashTable
{
public:
    using PKPairTyped = PKPair<Key, docid_t>;

public:
    // for load from buffer
    PrimaryKeyHashTable()
        : _pkCountPtr(NULL)
        , _docCountPtr(NULL)
        , _bucketCountPtr(NULL)
        , _bucketPtr(NULL)
        , _pkPairPtr(NULL)
        , _bucketCount(0)
    {
    }

    ~PrimaryKeyHashTable() {}

public:
    // for read from buffer
    void Init(char* buffer)
    {
        assert(buffer);
        _pkCountPtr = (uint64_t*)buffer;
        _docCountPtr = _pkCountPtr + 1;
        _bucketCountPtr = _docCountPtr + 1;
        _bucketCount = *_bucketCountPtr;
        _pkPairPtr = (PKPairTyped*)(_bucketCountPtr + 1);
        _bucketPtr = (docid_t*)(_pkPairPtr + *_docCountPtr);
    }

    // for write to buffer
    // docCount actually indicate the specified doc range,
    // so user must make sure that the inserted docId is in the range of [0, docCount).
    void Init(char* buffer, uint64_t pkCount, uint64_t docCount)
    {
        assert(buffer);
        _pkCountPtr = (uint64_t*)buffer;
        *_pkCountPtr = pkCount;
        _docCountPtr = _pkCountPtr + 1;
        *_docCountPtr = docCount;
        _bucketCountPtr = _docCountPtr + 1;
        *_bucketCountPtr = GetBucketCount(pkCount);
        _bucketCount = *_bucketCountPtr;
        _pkPairPtr = (PKPairTyped*)(_bucketCountPtr + 1);
        _bucketPtr = (docid_t*)(_pkPairPtr + *_docCountPtr);

        InitBucket();
        InitPkPairs(docCount);
    }

    void Insert(const PKPairTyped& pkPair)
    {
        indexlib::util::KeyHash<Key> hashFun;
        uint64_t bucketIdx = hashFun(pkPair.key) % _bucketCount;
        _pkPairPtr[pkPair.docid].key = pkPair.key;
        _pkPairPtr[pkPair.docid].docid = _bucketPtr[bucketIdx];
        _bucketPtr[bucketIdx] = pkPair.docid;
    }

    docid_t Find(const Key& key) const
    {
        // for uint128_t mod
        indexlib::util::KeyHash<Key> hashFun;
        uint64_t bucketIdx = hashFun(key) % _bucketCount;
        docid_t next = _bucketPtr[bucketIdx];
        while (next != INVALID_DOCID) {
            const PKPairTyped& pkPair = _pkPairPtr[next];
            if (likely(pkPair.key == key)) {
                return next;
            }
            next = pkPair.docid;
        }
        return INVALID_DOCID;
    }

public:
    static bool SeekToPkPair(const indexlib::file_system::FileReaderPtr& fileReader, uint64_t& pkCount)
    {
        assert(fileReader);
        uint64_t docCount;
        uint64_t bucketCount;
        if (fileReader->Read(&pkCount, sizeof(uint64_t)).GetOrThrow() != sizeof(uint64_t)) {
            AUTIL_LOG(ERROR, "read pkCount fail. ");
            return false;
        }
        if (sizeof(uint64_t) != fileReader->Read(&docCount, sizeof(uint64_t)).GetOrThrow()) {
            AUTIL_LOG(ERROR, "read docCount fail. ");
            return false;
        }
        if (sizeof(uint64_t) != fileReader->Read(&bucketCount, sizeof(uint64_t)).GetOrThrow()) {
            AUTIL_LOG(ERROR, "read bucketCount fail. ");
            return false;
        }
        return true;
    }
    static size_t CalculateMemorySize(uint64_t pkCount, uint64_t docCount);
    static bool IsInvalidPkPair(const PKPairTyped& pkPair);
    static size_t EstimatePkCount(size_t fileLength, uint32_t docCount);
    static size_t EstimateMemoryCostPerDoc() { return sizeof(PKPairTyped) + 2 * sizeof(docid_t); }

private:
    static uint64_t GetBucketCount(size_t pkCount)
    {
        return indexlib::util::PrimeNumberTable::FindPrimeNumber(int64_t(pkCount * BUCKET_COUNT_FACTOR));
    }

    void InitBucket()
    {
        for (uint64_t i = 0; i < _bucketCount; ++i) {
            _bucketPtr[i] = INVALID_DOCID;
        }
    }

    void InitPkPairs(uint64_t docCount)
    {
        if (*_pkCountPtr < docCount) {
            for (docid_t docid = 0; (size_t)docid < docCount; ++docid) {
                _pkPairPtr[docid] = INVALID_PK_PAIR;
            }
        }
    }

private:
    static docid_t NON_EXIST_DOCID;
    static PKPairTyped INVALID_PK_PAIR;
    static double BUCKET_COUNT_FACTOR;

public:
    uint64_t* _pkCountPtr;
    uint64_t* _docCountPtr;
    uint64_t* _bucketCountPtr;
    docid_t* _bucketPtr;
    PKPairTyped* _pkPairPtr;
    uint64_t _bucketCount; // cache for performance

private:
    AUTIL_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////////////

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, PrimaryKeyHashTable, T);

template <typename Key>
docid_t PrimaryKeyHashTable<Key>::NON_EXIST_DOCID = (docid_t)-2;

template <typename Key>
typename PrimaryKeyHashTable<Key>::PKPairTyped PrimaryKeyHashTable<Key>::INVALID_PK_PAIR = {Key(), NON_EXIST_DOCID};

template <typename Key>
double PrimaryKeyHashTable<Key>::BUCKET_COUNT_FACTOR = (double)5 / 3;

template <typename Key>
size_t PrimaryKeyHashTable<Key>::CalculateMemorySize(uint64_t pkCount, uint64_t docCount)
{
    // pk count, doc count, bucket count
    size_t headerSize = sizeof(uint64_t) * 3;
    size_t pkPairVecSize = sizeof(PKPairTyped) * docCount;
    size_t bucketSize = sizeof(docid_t) * GetBucketCount(pkCount);
    return headerSize + pkPairVecSize + bucketSize;
}

template <typename Key>
bool PrimaryKeyHashTable<Key>::IsInvalidPkPair(const PKPairTyped& pkPair)
{
    return pkPair.docid == NON_EXIST_DOCID;
}

template <typename Key>
size_t PrimaryKeyHashTable<Key>::EstimatePkCount(size_t fileLength, uint32_t docCount)
{
    size_t headerSize = sizeof(uint64_t) * 3;
    size_t pkPairVecSize = sizeof(PKPairTyped) * docCount;
    size_t bucketSize = fileLength - headerSize - pkPairVecSize;
    return std::min((size_t)std::ceil(bucketSize / BUCKET_COUNT_FACTOR), (size_t)docCount);
}
}} // namespace indexlibv2::index
