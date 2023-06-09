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
#include "indexlib/document/kkv/KKVKeysExtractor.h"

#include "autil/ConstString.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/config/FieldConfig.h"
#include "indexlib/document/kv/KVDocument.h"
#include "indexlib/index/common/KeyHasherWrapper.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"

using namespace std;

using namespace indexlib::util;
namespace indexlibv2::document {
AUTIL_LOG_SETUP(indexlib.document, KKVKeysExtractor);

KKVKeysExtractor::KKVKeysExtractor(const std::shared_ptr<config::KKVIndexConfig>& indexConfig)
    : _prefixKeyFieldType(indexConfig->GetPrefixFieldConfig()->GetFieldType())
    , _prefixKeyUseNumberHash(indexConfig->UseNumberHash())
    , _suffixKeyFieldType(indexConfig->GetSuffixFieldConfig()->GetFieldType())
{
}

KKVKeysExtractor::~KKVKeysExtractor() {}

void KKVKeysExtractor::HashPrefixKey(const std::string& pkey, uint64_t& pkeyHash)
{
    autil::StringView pkeyStr(pkey);
    bool ret = indexlib::index::KeyHasherWrapper::GetHashKey(_prefixKeyFieldType, _prefixKeyUseNumberHash,
                                                             pkeyStr.data(), pkeyStr.size(), pkeyHash);
    assert(ret);
    (void)ret;
}

void KKVKeysExtractor::HashSuffixKey(const std::string& suffixKey, uint64_t& suffixKeyHash)
{
    autil::StringView suffixKeyStr(suffixKey);
    bool ret = indexlib::index::KeyHasherWrapper::GetHashKey(_suffixKeyFieldType, /*useNumberHash=*/true,
                                                             suffixKey.data(), suffixKey.size(), suffixKeyHash);
    assert(ret);
    (void)ret;
}

} // namespace indexlibv2::document
