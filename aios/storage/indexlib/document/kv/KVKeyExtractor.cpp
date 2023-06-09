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
#include "indexlib/document/kv/KVKeyExtractor.h"

#include "autil/StringView.h"
#include "indexlib/index/common/KeyHasherWrapper.h"

using namespace std;

namespace indexlibv2 { namespace document {
AUTIL_LOG_SETUP(indexlib.document, KVKeyExtractor);

KVKeyExtractor::KVKeyExtractor(FieldType fieldType, bool useNumberHash)
    : _fieldType(fieldType)
    , _useNumberHash(useNumberHash)
{
}

KVKeyExtractor::~KVKeyExtractor() {}

void KVKeyExtractor::HashKey(const std::string& key, uint64_t& keyHash) const
{
    HashKey(autil::StringView(key), keyHash);
}

void KVKeyExtractor::HashKey(const autil::StringView& key, uint64_t& hash) const
{
    bool ret = indexlib::index::KeyHasherWrapper::GetHashKey(_fieldType, _useNumberHash, key.data(), key.size(), hash);
    assert(ret);
    (void)ret;
}

}} // namespace indexlibv2::document
