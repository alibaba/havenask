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
#include "indexlib/util/KeyHasherTyped.h"
namespace indexlib { namespace util {

AUTIL_LOG_SETUP(indexlib.util, LayerTextHasher);

/////////////////////////////TextHasher///////////////////////////////

bool TextHasher::GetHashKey(const char* word, size_t size, dictkey_t& key)
{
    key = autil::HashAlgorithm::hashString64(word, size);
    return true;
}

bool TextHasher::GetHashKey(const autil::StringView& word, dictkey_t& key)
{
    key = autil::HashAlgorithm::hashString64(word.data(), word.size());
    return true;
}

/////////////////////////////MurmurHasher///////////////////////////////
bool MurmurHasher::GetHashKey(const char* word, size_t size, dictkey_t& key, uint16_t salt)
{
    key = InnerGetHashKey(word, size, salt);
    return true;
}

bool MurmurHasher::GetHashKey(const autil::StringView& key, dictkey_t& hashKey, uint16_t salt)
{
    hashKey = InnerGetHashKey(key.data(), key.size(), salt);
    return true;
}

uint64_t MurmurHasher::InnerGetHashKey(const void* key, int len, uint64_t salt)
{
    if (salt == NO_SALT) {
        return autil::MurmurHash::MurmurHash64A(key, len, SEED);
    }
    return autil::MurmurHash::MurmurHash64A(key, len, SEED, salt);
}

}} // namespace indexlib::util
