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
#include "indexlib/index/common/KeyHasherWrapper.h"

namespace indexlib::index {

bool KeyHasherWrapper::IsOriginalKeyValid(FieldType fieldType, PrimaryKeyHashType hashType, const char* key,
                                          size_t size, bool hash64)
{
    switch (hashType) {
    case pk_default_hash:
        return util::DefaultHasher::IsOriginalKeyValid(key, size, hash64);
    case pk_number_hash:
        return IsOriginalKeyValid(fieldType, key, size, hash64);
    case pk_murmur_hash:
        return util::MurmurHasher::IsOriginalKeyValid(key, size, hash64);
    default:
        assert(false);
        return false;
    }
    assert(false);
    return false;
}

bool KeyHasherWrapper::GetHashKey(FieldType fieldType, PrimaryKeyHashType hashType, const char* key, size_t size,
                                  dictkey_t& hashKey)
{
    switch (hashType) {
    case pk_default_hash:
        return util::DefaultHasher::GetHashKey(key, size, hashKey);
    case pk_number_hash:
        return GetHashKeyByFieldType(fieldType, key, size, hashKey);
    case pk_murmur_hash:
        return util::MurmurHasher::GetHashKey(key, size, hashKey);
    default:
        assert(false);
        return false;
    }
    assert(false);
    return false;
}

bool KeyHasherWrapper::GetHashKey(FieldType fieldType, PrimaryKeyHashType hashType, const char* key, size_t size,
                                  autil::uint128_t& hashKey)
{
    assert(hashType == pk_default_hash or hashType == pk_number_hash or hashType == pk_murmur_hash);
    hashKey = autil::HashAlgorithm::hashString128(key, size);
    return true;
}

bool KeyHasherWrapper::GetHashKey(const char* word, size_t size, autil::uint128_t& key)
{
    key = autil::HashAlgorithm::hashString128(word, size);
    return true;
}

bool KeyHasherWrapper::GetHashKeyByInvertedIndexType(InvertedIndexType indexType, const char* key, size_t size,
                                                     dictkey_t& hashKey, int salt)
{
    switch (indexType) {
    case it_number_int64:
        return util::Int64NumberHasher::GetHashKey(key, size, hashKey, salt);
    case it_number_uint64:
        return util::UInt64NumberHasher::GetHashKey(key, size, hashKey, salt);
    case it_number_int32:
        return util::Int32NumberHasher::GetHashKey(key, size, hashKey, salt);
    case it_number_uint32:
        return util::UInt32NumberHasher::GetHashKey(key, size, hashKey, salt);
    case it_number_int16:
        return util::Int16NumberHasher::GetHashKey(key, size, hashKey, salt);
    case it_number_uint16:
        return util::UInt16NumberHasher::GetHashKey(key, size, hashKey, salt);
    case it_number_int8:
        return util::Int8NumberHasher::GetHashKey(key, size, hashKey, salt);
    case it_number_uint8:
        return util::UInt8NumberHasher::GetHashKey(key, size, hashKey, salt);
    case it_spatial:
        return util::UInt64NumberHasher::GetHashKey(key, size, hashKey, salt);
    case it_unknown:
        assert(false);
        return false;
    default:
        return util::DefaultHasher::GetHashKey(key, size, hashKey);
    }
    assert(false);
    return false;
}

bool KeyHasherWrapper::GetHashKeyByFieldType(FieldType fieldType, const char* key, size_t size, dictkey_t& hashKey)
{
    switch (fieldType) {
    case ft_int64:
        return util::Int64NumberHasher::GetHashKey(key, size, hashKey);
    case ft_uint64:
        return util::UInt64NumberHasher::GetHashKey(key, size, hashKey);
    case ft_int32:
        return util::Int32NumberHasher::GetHashKey(key, size, hashKey);
    case ft_uint32:
        return util::UInt32NumberHasher::GetHashKey(key, size, hashKey);
    case ft_int16:
        return util::Int16NumberHasher::GetHashKey(key, size, hashKey);
    case ft_uint16:
        return util::UInt16NumberHasher::GetHashKey(key, size, hashKey);
    case ft_int8:
        return util::Int8NumberHasher::GetHashKey(key, size, hashKey);
    case ft_uint8:
        return util::UInt8NumberHasher::GetHashKey(key, size, hashKey);
    case ft_location:
        return util::UInt64NumberHasher::GetHashKey(key, size, hashKey);
    case ft_unknown:
        assert(false);
        return false;
    default:
        return util::DefaultHasher::GetHashKey(key, size, hashKey);
    }
    assert(false);
    return false;
}

bool KeyHasherWrapper::IsOriginalKeyValid(FieldType fieldType, const char* key, size_t size, bool hash64)
{
    switch (fieldType) {
    case ft_int64:
        return util::Int64NumberHasher::IsOriginalKeyValid(key, size, hash64);
    case ft_uint64:
        return util::UInt64NumberHasher::IsOriginalKeyValid(key, size, hash64);
    case ft_int32:
        return util::Int32NumberHasher::IsOriginalKeyValid(key, size, hash64);
    case ft_uint32:
        return util::UInt32NumberHasher::IsOriginalKeyValid(key, size, hash64);
    case ft_int16:
        return util::Int16NumberHasher::IsOriginalKeyValid(key, size, hash64);
    case ft_uint16:
        return util::UInt16NumberHasher::IsOriginalKeyValid(key, size, hash64);
    case ft_int8:
        return util::Int8NumberHasher::IsOriginalKeyValid(key, size, hash64);
    case ft_uint8:
        return util::UInt8NumberHasher::IsOriginalKeyValid(key, size, hash64);
    case ft_location:
        return util::UInt64NumberHasher::IsOriginalKeyValid(key, size, hash64);
    default: {
        assert(false);
        return false;
    }
    }
    assert(false);
    return false;
}

bool KeyHasherWrapper::GetHashKey(FieldType fieldType, bool useNumberHash, const char* key, size_t size,
                                  dictkey_t& hashKey)
{
    if (!useNumberHash) {
        return util::MurmurHasher::GetHashKey(key, size, hashKey);
    }
    switch (fieldType) {
    case ft_int64:
    case ft_int32:
    case ft_int16:
    case ft_int8:
        return util::Int64NumberHasher::GetHashKey(key, size, hashKey);
    case ft_uint64:
    case ft_uint32:
    case ft_uint16:
    case ft_uint8:
        return util::UInt64NumberHasher::GetHashKey(key, size, hashKey);
    default:
        return util::MurmurHasher::GetHashKey(key, size, hashKey);
    }
    return false;
}
} // namespace indexlib::index
