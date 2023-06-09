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
#ifndef __INDEXLIB_AUX_TABLE_READER_TYPED_H
#define __INDEXLIB_AUX_TABLE_READER_TYPED_H

#include <memory>

#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/index/common/KeyHasherWrapper.h"
#include "indexlib/index/kv/kv_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_typed.h"
#include "indexlib/index/primary_key/PrimaryKeyHashConvertor.h"
#include "indexlib/index/primary_key/PrimaryKeyReader.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace partition {

template <typename HashType = uint64_t>
using AuxTableReaderHasher = std::function<bool(const char*, size_t, HashType&)>;

template <typename T>
struct AuxTableReaderImpl {
public:
    AuxTableReaderImpl(index::PrimaryKeyIndexReader* pkIndexReader, index::AttributeIteratorTyped<T>* iterator)
        : pkReader(pkIndexReader)
        , attrIterator(iterator)
        , tableType(tt_index)
    {
    }

    AuxTableReaderImpl(index::KVReader* kvReader_, const std::string& fieldName_ = "",
                       autil::mem_pool::Pool* pool_ = nullptr)
        : kvReader(kvReader_)
        , tableType(tt_kv)
    {
        kvReaderOptions.pool = pool_;
        if (kvReader_ && !fieldName_.empty()) {
            kvReaderOptions.attrId = kvReader_->GetAttributeId(fieldName_);
        }
    }
    AuxTableReaderImpl(AuxTableReaderImpl&& other)
        : pkReader(other.pkReader)
        , attrIterator(other.attrIterator)
        , kvReader(other.kvReader)
        , kvReaderOptions(other.kvReaderOptions)
        , tableType(other.tableType)
    {
    }
    ~AuxTableReaderImpl() {}

public:
    bool GetValue(const autil::StringView& key, T& value, bool& isNull)
    {
        if (kvReader) {
            assert(tableType == tt_kv);
            isNull = false;
            return future_lite::interface::syncAwait(kvReader->GetAsync(key, value, kvReaderOptions));
        }

        assert(tableType == tt_index);
        assert(pkReader != nullptr);
        assert(attrIterator != nullptr);
        docid_t docId = pkReader->Lookup(key);
        if (docId == INVALID_DOCID) {
            return false;
        }
        return attrIterator->Seek(docId, value, isNull);
    }

    bool GetValue(uint64_t key, T& value, bool& isNull)
    {
        if (kvReader) {
            assert(tableType == tt_kv);
            isNull = false;
            return future_lite::interface::syncAwait(kvReader->GetAsync(key, value, kvReaderOptions));
        }
        assert(tableType == tt_index);
        assert(pkReader);
        docid_t docId = pkReader->LookupWithType(key, nullptr);
        return docId != INVALID_DOCID && attrIterator->Seek(docId, value, isNull);
    }

    bool GetValueWithPkHash(uint64_t key, T& value, bool& isNull)
    {
        if (kvReader) {
            assert(tableType == tt_kv);
            isNull = false;
            return kvReader->Get(key, value, kvReaderOptions);
        }

        assert(tableType == tt_index);
        assert(pkReader);
        autil::uint128_t key128;
        index::PrimaryKeyHashConvertor::ToUInt128(key, key128);
        docid_t docId = pkReader->LookupWithPKHash(key128);
        return docId != INVALID_DOCID && attrIterator->Seek(docId, value, isNull);
    }

    bool GetKVValue(uint64_t key, T& value, bool& isNull)
    {
        assert(kvReader);
        isNull = false;
        return kvReader->Get(key, value, kvReaderOptions);
    }

    template <typename HashType>
    AuxTableReaderHasher<HashType> GetHasher() const
    {
        if (kvReader) {
            HashFunctionType keyHasherType = kvReader->GetHasherType();
            return [keyHasherType](const char* str, size_t len, HashType& key) {
                dictkey_t dictKey = 0;
                bool ret = index::KVReader::GetHashKeyWithType(keyHasherType, autil::StringView(str, len), dictKey);
                if (ret) {
                    key = dictKey;
                }
                return ret;
            };
        }

        assert(pkReader);
        assert(tableType == tt_index);
        FieldType fieldType;
        index::PrimaryKeyHashType primaryKeyHashType;
        if (pkReader->GetInvertedIndexType() == it_primarykey64) {
            indexlibv2::index::PrimaryKeyReader<uint64_t>* reader =
                static_cast<indexlibv2::index::PrimaryKeyReader<uint64_t>*>(pkReader);
            fieldType = reader->GetFieldType();
            primaryKeyHashType = reader->GetPrimaryKeyHashType();
        } else if (pkReader->GetInvertedIndexType() == it_primarykey128) {
            indexlibv2::index::PrimaryKeyReader<autil::uint128_t>* reader =
                static_cast<indexlibv2::index::PrimaryKeyReader<autil::uint128_t>*>(pkReader);
            fieldType = reader->GetFieldType();
            primaryKeyHashType = reader->GetPrimaryKeyHashType();
        } else {
            assert(false);
            return nullptr;
        }
        return [fieldType, primaryKeyHashType](const char* str, size_t len, HashType& key) {
            return index::KeyHasherWrapper::GetHashKey(fieldType, primaryKeyHashType, str, len, key);
        };
    }

    HashFunctionType GetHasherType() const
    {
        if (kvReader) {
            return kvReader->GetHasherType();
        }
        return hft_unknown;
    }

public:
    index::PrimaryKeyIndexReader* pkReader = nullptr;
    index::AttributeIteratorTyped<T>* attrIterator = nullptr;
    index::KVReader* kvReader = nullptr;
    index::KVReadOptions kvReaderOptions;
    TableType tableType;
};

// class AuxTableReader {
// public:
//     AuxTableReader(const config::AttributeConfigPtr &attrConfig)
//         : mAttrConfig(attrConfig)
//     {}
//     virtual ~AuxTableReader() = default;
// public:
//     bool IsMulti() const { return mAttrConfig->IsMultiValue(); }
//     FieldType GetFieldType() const { return mAttrConfig->GetFieldType(); }
// private:
//     config::AttributeConfigPtr mAttrConfig;
// };

template <typename T>
class AuxTableReaderTyped
{
public:
    AuxTableReaderTyped(AuxTableReaderImpl<T>& impl, autil::mem_pool::Pool* pool = nullptr)
        : mImpl(std::move(impl))
        , mPool(pool)
    {
    }
    ~AuxTableReaderTyped() { IE_POOL_COMPATIBLE_DELETE_CLASS(mPool, mImpl.attrIterator); }

public:
    bool GetValue(const std::string& primaryKeyStr, T& value)
    {
        autil::StringView constKey(primaryKeyStr.data(), primaryKeyStr.length());
        return GetValue(constKey, value);
    }
    bool GetValue(uint64_t key, T& value)
    {
        bool isNull = false;
        return mImpl.GetValue(key, value, isNull);
    }
    bool GetValue(const autil::StringView& primaryKeyStr, T& value)
    {
        bool isNull = false;
        return mImpl.GetValue(primaryKeyStr, value, isNull);
    }
    bool GetValueWithPkHash(uint64_t key, T& value)
    {
        bool isNull = false;
        return mImpl.GetValueWithPkHash(key, value, isNull);
    }
    bool GetKVValue(uint64_t key, T& value)
    {
        bool isNull = false;
        return mImpl.GetKVValue(key, value, isNull);
    }

    // support isNull
    bool GetValue(const std::string& primaryKeyStr, T& value, bool& isNull)
    {
        autil::StringView constKey(primaryKeyStr.data(), primaryKeyStr.length());
        return GetValue(constKey, value, isNull);
    }

    bool GetValue(uint64_t key, T& value, bool& isNull) { return mImpl.GetValue(key, value, isNull); }

    bool GetValue(const autil::StringView& primaryKeyStr, T& value, bool& isNull)
    {
        return mImpl.GetValue(primaryKeyStr, value, isNull);
    }

    bool GetValueWithPkHash(uint64_t key, T& value, bool& isNull)
    {
        return mImpl.GetValueWithPkHash(key, value, isNull);
    }

    bool GetKVValue(uint64_t key, T& value, bool& isNull) { return mImpl.GetKVValue(key, value, isNull); }

    template <typename HashType = uint64_t>
    AuxTableReaderHasher<HashType> GetHasher() const
    {
        return mImpl.template GetHasher<HashType>();
    }

    HashFunctionType GetHasherType() const { return mImpl.GetHasherType(); }

public:
    // for test
    TableType GetTableType() const { return mImpl.tableType; }

private:
    AuxTableReaderImpl<T> mImpl;
    autil::mem_pool::Pool* mPool;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::partition

#endif //__INDEXLIB_AUX_TABLE_READER_TYPED_H
