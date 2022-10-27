#ifndef __INDEXLIB_AUX_TABLE_READER_TYPED_H
#define __INDEXLIB_AUX_TABLE_READER_TYPED_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader_typed.h"
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_typed.h"
#include <autil/StringUtil.h>

IE_NAMESPACE_BEGIN(partition);

template<typename HashType = uint64_t>
using AuxTableReaderHasher = std::function<bool(const char*, size_t, HashType&)>;

template <typename T>
struct AuxTableReaderImpl {
public:
    AuxTableReaderImpl(index::PrimaryKeyIndexReader *pkIndexReader,
                       index::AttributeIteratorTyped<T> *iterator)
        : pkReader(pkIndexReader)
        , attrIterator(iterator)
        , tableType(tt_index)
    {}

    AuxTableReaderImpl(AuxTableReaderImpl &&other)
        : pkReader(other.pkReader)
        , attrIterator(other.attrIterator)
        , tableType(other.tableType)
    {}
    ~AuxTableReaderImpl() {
    }
public:
    bool GetValue(const autil::ConstString &key, T &value) {
        assert(tableType == tt_index);
        assert(pkReader != nullptr);
        assert(attrIterator != nullptr);
        docid_t docId = pkReader->Lookup(key);
        if (docId == INVALID_DOCID) {
            return false;
        }
        return attrIterator->Seek(docId, value);
    }

    bool GetValue(uint64_t key, T &value) {
        assert(tableType == tt_index);
        assert(pkReader);
        docid_t docId = INVALID_DOCID;
        if (pkReader->GetIndexType() == it_primarykey64) {
            index::UInt64PrimaryKeyIndexReader *reader =
                static_cast<index::UInt64PrimaryKeyIndexReader *>(pkReader);
            docId = reader->LookupWithNumber(key);
        } else {
            index::UInt128PrimaryKeyIndexReader *reader =
                static_cast<index::UInt128PrimaryKeyIndexReader *>(pkReader);
            docId = reader->LookupWithNumber(key);
        }
        return docId != INVALID_DOCID && attrIterator->Seek(docId, value);
    }

    bool GetValueWithPkHash(uint64_t key, T &value) {
        assert(tableType == tt_index);
        assert(pkReader);
        autil::uint128_t key128;
        index::PrimaryKeyHashConvertor::ToUInt128(key, key128);
        docid_t docId = pkReader->LookupWithPKHash(key128);
        return docId != INVALID_DOCID && attrIterator->Seek(docId, value);
    }

    template<typename HashType>
    AuxTableReaderHasher<HashType> GetHasher() const {
        assert(pkReader);
        assert(tableType == tt_index);
        util::KeyHasherPtr keyHasher;
        if (pkReader->GetIndexType() == it_primarykey64) {
            index::UInt64PrimaryKeyIndexReader *reader =
                static_cast<index::UInt64PrimaryKeyIndexReader *>(pkReader);
            keyHasher = reader->GetHasher();
        } else {
            index::UInt128PrimaryKeyIndexReader *reader =
                static_cast<index::UInt128PrimaryKeyIndexReader *>(pkReader);
            keyHasher = reader->GetHasher();
        }
        return [keyHasher](const char* str, size_t len, HashType &key) {
            return keyHasher->GetHashKey(str, len, key);
        };
    }

    bool GetKVValue(uint64_t key, T &value) {
        return false;
    }

    HashFunctionType GetHasherType() const {
        return hft_unknown;
    }

public:
    index::PrimaryKeyIndexReader *pkReader = nullptr;
    index::AttributeIteratorTyped<T> *attrIterator = nullptr;
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
    AuxTableReaderTyped(AuxTableReaderImpl<T> &impl,
                        autil::mem_pool::Pool *pool = nullptr)
        : mImpl(std::move(impl))
        , mPool(pool)
    {
    }
    ~AuxTableReaderTyped() {
        IE_POOL_COMPATIBLE_DELETE_CLASS(mPool, mImpl.attrIterator);
    }
public:
    bool GetValue(const std::string &primaryKeyStr, T &value) {
        autil::ConstString constKey(primaryKeyStr.data(), primaryKeyStr.length());
        return GetValue(constKey, value);
    }
    bool GetValue(uint64_t key, T &value) {
        return mImpl.GetValue(key, value);
    }
    bool GetValue(const autil::ConstString &primaryKeyStr, T &value) {
        return mImpl.GetValue(primaryKeyStr, value);
    }
    bool GetValueWithPkHash(uint64_t key, T &value) {
        return mImpl.GetValueWithPkHash(key, value);
    }
    bool GetKVValue(uint64_t key, T &value) {
        return mImpl.GetKVValue(key, value);
    }

    template<typename HashType = uint64_t>
    AuxTableReaderHasher<HashType> GetHasher() const { return mImpl.template GetHasher<HashType>(); }

    HashFunctionType GetHasherType() const {
        return mImpl.GetHasherType();
    }
public:
    // for test
    TableType GetTableType() const {
        return mImpl.tableType;
    }
private:
    AuxTableReaderImpl<T> mImpl;
    autil::mem_pool::Pool *mPool;
private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_AUX_TABLE_READER_TYPED_H
