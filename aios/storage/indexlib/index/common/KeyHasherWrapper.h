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

#include "indexlib/base/FieldType.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/common/PrimaryKeyHashType.h"
#include "indexlib/index/common/Types.h"
#include "indexlib/util/KeyHasherTyped.h"

namespace indexlib::index {

class KeyHasherWrapper
{
public:
    static bool GetHashKey(FieldType fieldType, PrimaryKeyHashType hashType, const char* key, size_t size,
                           dictkey_t& hashKey);
    static bool GetHashKey(FieldType fieldType, PrimaryKeyHashType hashType, const char* key, size_t size,
                           autil::uint128_t& hashKey);
    static bool GetHashKey(const char* word, size_t size, autil::uint128_t& key);

    static bool IsOriginalKeyValid(FieldType fieldType, PrimaryKeyHashType hashType, const char* key, size_t size,
                                   bool hash64);
    static bool IsOriginalKeyValid(FieldType fieldType, const char* key, size_t size, bool hash64);

    static bool GetHashKeyByInvertedIndexType(InvertedIndexType indexType, const char* key, size_t size,
                                              dictkey_t& hashKey, int salt = util::NumberHasher::DEFAULT_SALT);

    static bool GetHashKeyByFieldType(FieldType fieldType, const char* key, size_t size, dictkey_t& hashKey);

    static bool GetHashKey(FieldType fieldType, bool useNumberHash, const char* key, size_t size, dictkey_t& hashKey);

private:
    KeyHasherWrapper();
};

} // namespace indexlib::index
