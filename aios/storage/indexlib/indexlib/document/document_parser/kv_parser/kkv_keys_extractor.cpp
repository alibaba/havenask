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
#include "indexlib/document/document_parser/kv_parser/kkv_keys_extractor.h"

#include "indexlib/common/multi_region_rehasher.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/index/common/KeyHasherWrapper.h"

using namespace indexlib::config;
using namespace indexlib::document;
using namespace indexlib::common;

using namespace indexlib::util;
namespace indexlib { namespace document {
IE_LOG_SETUP(document, KKVKeysExtractor);

KKVKeysExtractor::KKVKeysExtractor(const KKVIndexConfigPtr& kkvConfig)
    : _regionId(kkvConfig->GetRegionCount() > 1 ? kkvConfig->GetRegionId() : INVALID_REGIONID)
    , _prefixKeyFieldType(kkvConfig->GetPrefixFieldConfig()->GetFieldType())
    , _prefixKeyUseNumberHash(kkvConfig->UseNumberHash())
    , _suffixKeyFieldType(kkvConfig->GetSuffixFieldConfig()->GetFieldType())

{
}

KKVKeysExtractor::~KKVKeysExtractor() {}

bool KKVKeysExtractor::GetKeys(document::KVIndexDocument* doc, uint64_t& pkey, uint64_t& skey,
                               OperationType& operationType)
{
    if (_regionId != INVALID_REGIONID && _regionId != doc->GetRegionId()) {
        ERROR_COLLECTOR_LOG(ERROR, "region id miss match between doc [%d] and schema [%d]", doc->GetRegionId(),
                            _regionId);
        return false;
    }

    pkey = doc->GetPKeyHash();
    skey = doc->GetSKeyHash();

    if (_regionId != INVALID_REGIONID) {
        pkey = MultiRegionRehasher::GetRehashKey(pkey, _regionId);
    }

    auto opType = doc->GetDocOperateType();
    if (doc->HasSKey()) {
        operationType = (opType == ADD_DOC) ? ADD : DELETE_SKEY;
        return true;
    }

    if (opType != DELETE_DOC) {
        ERROR_COLLECTOR_LOG(ERROR, "add doc with pkey hash [%lu] has no skey", doc->GetPKeyHash());
        return false;
    }
    operationType = DELETE_PKEY;
    return true;
}

bool KKVKeysExtractor::HashPrefixKey(const std::string& pkey, uint64_t& pkeyHash)
{
    autil::StringView pkeyStr(pkey);
    return index::KeyHasherWrapper::GetHashKey(_prefixKeyFieldType, _prefixKeyUseNumberHash, pkeyStr.data(),
                                               pkeyStr.size(), pkeyHash);
}

bool KKVKeysExtractor::HashSuffixKey(const std::string& suffixKey, uint64_t& suffixKeyHash)
{
    autil::StringView suffixKeyStr(suffixKey);
    return index::KeyHasherWrapper::GetHashKey(_suffixKeyFieldType, /*useNumberHash=*/true, suffixKey.data(),
                                               suffixKey.size(), suffixKeyHash);
}
}} // namespace indexlib::document
